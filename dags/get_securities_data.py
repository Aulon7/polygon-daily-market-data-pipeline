import logging
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from lib.eod_data_downloader import download_polygon_eod_data_for_date


# Runtime defaults
DEFAULT_S3_STAGE_PREFIX = "market/bronze/"
EXCHANGE_TZ = "America/New_York"
MIN_EXPECTED_TICKERS = 100
S3_EOD_SUBPATH = "eod/eod_prices_{trading_date}.csv"

log = logging.getLogger(__name__)


def on_failure_callback(context):
    """
    Custom callback for DAG failures 
    """
    task_instance = context.get('task_instance')
    log.error(f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id}")


def _get_runtime_config() -> tuple[str, str, str]:
    s3_bucket = Variable.get("S3_BUCKET", default_var=None)
    polygon_api_key = Variable.get("POLYGON_API_KEY", default_var=None)
    s3_stage_prefix = Variable.get("SNOWFLAKE_STAGE_PREFIX", default_var=DEFAULT_S3_STAGE_PREFIX)

    if not s3_bucket:
        raise AirflowException("Missing Airflow Variable: S3_BUCKET. Set it before running the DAG.")
    if not polygon_api_key:
        raise AirflowException("Missing Airflow Variable: POLYGON_API_KEY. Set it before running the DAG.")

    return s3_bucket, polygon_api_key, s3_stage_prefix


def _normalize_stage_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _build_s3_key(stage_prefix: str, trading_date: str) -> str:
    normalized_prefix = _normalize_stage_prefix(stage_prefix)
    return f"{normalized_prefix}{S3_EOD_SUBPATH.format(trading_date=trading_date)}"


def _build_snowflake_key(s3_full_key: str, stage_prefix: str) -> str:
    if s3_full_key.startswith(stage_prefix):
        return s3_full_key[len(stage_prefix):]

    log.warning(
        "S3 key '%s' doesn't start with expected prefix '%s'. Using full key for Snowflake.",
        s3_full_key,
        stage_prefix,
    )
    return s3_full_key


def _validate_row_count(trading_date: str, row_count: int) -> None:
    if row_count < MIN_EXPECTED_TICKERS:
        raise AirflowException(
            f"Data quality check failed: Only {row_count} tickers found for {trading_date}. "
            f"Expected at least {MIN_EXPECTED_TICKERS}."
        )


@dag(
    dag_id='polygon_modern_elt_v2',
    schedule='0 0 * * *',  # 00:00 UTC daily
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    template_searchpath=["/opt/airflow/dags/sql"],
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=1),
        "on_failure_callback": on_failure_callback,
    },
    tags=["polygon", "snowflake", "taskflow", "elt", "v2"],
    description="Modern ELT pipeline: Polygon API → S3 → Snowflake (Raw → Core → Dimensional Model)",
)
def financial_pipeline():
    """
    Financial data pipeline that:
    1. Downloads EOD stock prices from Polygon.io
    2. Uploads to S3 in CSV format
    3. Loads into Snowflake RAW layer
    4. Processes through CORE and dimensional model (DM_DIM/DM_FACT)
    """

    @task
    def upload_api_data_to_s3():
        """
        Downloads Polygon EOD data and uploads to S3.
        Returns metadata for downstream Snowflake tasks.
        """
        s3_bucket, polygon_api_key, s3_stage_prefix = _get_runtime_config()

        trading_date = pendulum.now(EXCHANGE_TZ).date().strftime("%Y-%m-%d")
        csv_content, row_count = download_polygon_eod_data_for_date(
            polygon_api_key,
            trading_date,
        )

        if row_count is None or row_count == 0 or csv_content is None:
            raise AirflowSkipException(
                f"No valid trading data for {trading_date}."
            )

        row_count = int(row_count)
        _validate_row_count(trading_date, row_count)

        s3 = S3Hook(aws_conn_id="aws_default")
        s3_stage_prefix = _normalize_stage_prefix(s3_stage_prefix)
        s3_full_key = _build_s3_key(s3_stage_prefix, trading_date)

        if s3.check_for_key(key=s3_full_key, bucket_name=s3_bucket):
            raise AirflowSkipException(
                f"S3 object already exists, skipping upload: s3://{s3_bucket}/{s3_full_key}"
            )

        log.info(f"Uploading {row_count:,} rows to s3://{s3_bucket}/{s3_full_key}")
        s3.load_string(
            csv_content,
            key=s3_full_key,
            bucket_name=s3_bucket,
            replace=False,
        )
        
        snowflake_relative_key = _build_snowflake_key(s3_full_key, s3_stage_prefix)
        
        log.info(f"Trading date: {trading_date}, Rows: {row_count:,}, Snowflake key: {snowflake_relative_key}")
        
        return {
            "s3_key": snowflake_relative_key,
            "trading_date": trading_date,
            "row_count": row_count,
            "s3_full_path": f"s3://{s3_bucket}/{s3_full_key}"
        }
    
    @task
    def validate_metadata(metadata: dict):
        """
        Additional validation gate before Snowflake processing.
        """
        log.info(f"Validating metadata: {metadata}")
        
        required_keys = ["s3_key", "trading_date", "row_count"]
        missing = [k for k in required_keys if k not in metadata]
        
        if missing:
            raise AirflowException(f"Missing required metadata keys: {missing}")
        
        _validate_row_count(metadata["trading_date"], metadata["row_count"])
        
        log.info(f"✓ Validation passed - {metadata['row_count']} rows ready for processing")
        return metadata
        
    # Execute upload and validation
    metadata = upload_api_data_to_s3()
    validated_metadata = validate_metadata(metadata)

    # Snowflake processing task group
    with TaskGroup(group_id="snowflake_processing") as snowflake_load:
    
        copy_to_raw = SQLExecuteQueryOperator(
            task_id="raw_01_copy_from_s3",
            conn_id="snowflake_default",
            sql="copy_to_raw.sql",
            autocommit=True,
        )

        check_loaded = SQLExecuteQueryOperator(
            task_id="raw_02_validate_load",
            conn_id="snowflake_default",
            sql="check_loaded.sql",
            autocommit=True,
        )

        premerge_metrics = SQLExecuteQueryOperator(
            task_id="raw_03_premerge_metrics",
            conn_id="snowflake_default",
            sql="premerge_metrics.sql",
            autocommit=True,
        )

        merge_core = SQLExecuteQueryOperator(
            task_id="core_01_merge",
            conn_id="snowflake_default",
            sql="merge_core.sql",
            autocommit=True,
        )

        merge_dim_security = SQLExecuteQueryOperator(
            task_id="dim_01_merge_securities",
            conn_id="snowflake_default",
            sql="merge_dim_security.sql",
            autocommit=True,
        )

        merge_dim_date = SQLExecuteQueryOperator(
            task_id="dim_02_merge_dates",
            conn_id="snowflake_default",
            sql="merge_dim_date.sql",
            autocommit=True,
        )

        merge_facts = SQLExecuteQueryOperator(
            task_id="fact_01_merge_daily_prices",
            conn_id="snowflake_default",
            sql="merge_facts_daily.sql",
            autocommit=True,
        )

        postmerge_metrics = SQLExecuteQueryOperator(
            task_id="metrics_01_postmerge_validation",
            conn_id="snowflake_default",
            sql="postmerge_metrics.sql",
            autocommit=True,
        )

        copy_to_raw >> check_loaded >> premerge_metrics >> merge_core
        merge_core >> [merge_dim_security, merge_dim_date] >> merge_facts >> postmerge_metrics

    # Overall DAG flow
    validated_metadata >> snowflake_load


financial_pipeline()