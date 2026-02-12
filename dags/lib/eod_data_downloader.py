import csv
import io
import logging
from typing import Iterable, Tuple

import pendulum
import requests
from airflow.exceptions import AirflowException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

POLYGON_BASE_URL = "https://api.polygon.io"
EXCHANGE_TZ = "America/New_York"
REQUEST_TIMEOUT = 30
MIN_EXPECTED_TICKERS = 100  # Minimum tickers for a valid trading day
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
RETRY_ALLOWED_METHODS = ("GET", "POST")


def download_polygon_eod_data_to_s3_stream(
    polygon_api_key: str, 
    lookback_days: int
) -> Tuple[str, str, int]:
    """Download the most recent valid EOD data within the lookback window."""
    if not polygon_api_key:
        raise AirflowException("Missing Polygon API Key. Set POLYGON_API_KEY in Airflow Variables.")

    today = pendulum.now(EXCHANGE_TZ).date()
    log.info(
        "Starting Polygon EOD download. Today: %s, Lookback: %d",
        today.to_date_string(),
        lookback_days,
    )

    for offset in range(0, max(lookback_days, 0) + 1):
        trading_date = today.subtract(days=offset).to_date_string()
        csv_string, row_count = download_polygon_eod_data_for_date(
            polygon_api_key,
            trading_date,
        )
        if row_count and csv_string:
            log.info(
                "Generated CSV: %s bytes, %s data rows for %s",
                f"{len(csv_string):,}",
                f"{row_count:,}",
                trading_date,
            )
            return csv_string, trading_date, row_count

    raise AirflowException(
        "No valid trading data found within lookback window. "
        "This could indicate: (1) Market closed, (2) API issues, or (3) Invalid API key."
    )


def download_polygon_eod_data_for_date(
    polygon_api_key: str,
    trading_date: str,
) -> Tuple[str | None, int]:
    """Download Polygon EOD data for a specific trading date."""
    if not polygon_api_key:
        raise AirflowException("Missing Polygon API Key. Set POLYGON_API_KEY in Airflow Variables.")

    url = f"{POLYGON_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{trading_date}"
    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": polygon_api_key,
    }

    try:
        session = _build_session()
        response = _request_json(session, url, params, trading_date)
        if response is None:
            return None, 0
    except requests.exceptions.Timeout:
        log.warning("Request timeout for %s after %ss.", trading_date, REQUEST_TIMEOUT)
        return None, 0
    except requests.exceptions.RequestException as e:
        log.warning("Request failed for %s: %s.", trading_date, e)
        return None, 0

    results = _extract_results(response, trading_date)
    if not results:
        return None, 0

    csv_string = _convert_to_csv(results, trading_date)
    return csv_string, len(results)


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=list(RETRY_STATUS_CODES),
        allowed_methods=list(RETRY_ALLOWED_METHODS),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _request_json(
    session: requests.Session,
    url: str,
    params: dict,
    trading_date: str,
) -> requests.Response | None:
    log.info("[Polygon API] Requesting data for %s...", trading_date)
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    log.info("[Polygon API] %s -> HTTP %s", trading_date, response.status_code)
    if response.status_code != 200:
        log.warning(
            "Non-200 status for %s: %s. Response: %s",
            trading_date,
            response.status_code,
            response.text[:200],
        )
        return None
    return response


def _extract_results(response: requests.Response, trading_date: str) -> list[dict] | None:
    try:
        data = response.json()
    except ValueError as e:
        log.warning("Invalid JSON response for %s: %s", trading_date, e)
        return None

    results_count = data.get("resultsCount", 0)
    results = data.get("results", [])
    if results_count == 0 or not results:
        log.info("No data for %s.", trading_date)
        return None

    if results_count < MIN_EXPECTED_TICKERS:
        log.warning(
            "Only %s tickers found for %s. Expected at least %s.",
            results_count,
            trading_date,
            MIN_EXPECTED_TICKERS,
        )
        return None

    return results


def _convert_to_csv(results: Iterable[dict], trading_date: str) -> str:
    """Convert Polygon results to CSV content."""
    polygon_fields = ["T", "o", "h", "l", "c", "v"]
    csv_header = ["trade_date", "symbol", "open", "high", "low", "close", "volume"]
    
    with io.StringIO() as output:
        writer = csv.writer(output)
        writer.writerow(csv_header)

        for ticker_data in results:
            row = [trading_date] + [ticker_data.get(field, "") for field in polygon_fields]
            writer.writerow(row)

        return output.getvalue()


def validate_polygon_connection(polygon_api_key: str) -> bool:
    """Validate Polygon API connectivity."""
    try:
        url = f"{POLYGON_BASE_URL}/v3/reference/tickers"
        params = {"apiKey": polygon_api_key, "limit": 1}
        
        session = _build_session()
        response = session.get(url, params=params, timeout=10)

        if response.status_code == 200:
            log.info("Polygon API connection successful")
            return True
        else:
            log.error("Polygon API connection failed: HTTP %s", response.status_code)
            return False

    except Exception as e:
        log.error("Polygon API connection test failed: %s", e)
        return False