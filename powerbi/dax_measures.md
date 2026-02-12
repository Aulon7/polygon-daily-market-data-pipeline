- Active Tickers: Distinct count of traded symbols.
	```DAX
	Active Tickers = DISTINCTCOUNT(dim_security[SYMBOL])
	```

- Avg Price: Average close price across the current filter context.
	```DAX
	Avg Price = AVERAGE(fact_daily_price[CLOSE])
	```

- Daily Change %: Percent change vs the prior day close.
	```DAX
	Daily Change % =
	VAR _CurrentPrice = SUM(fact_daily_price[DATE_SK])
	VAR _PreviousPrice =
			CALCULATE(
					SUM(fact_daily_price[CLOSE]),
					DATEADD(dim_date[CAL_DATE], -1, DAY)
			)
	RETURN DIVIDE(_CurrentPrice - _PreviousPrice, _PreviousPrice, 0)
	```

- Total Value: Sum of volume * close price.
	```DAX
	Total Value = SUMX(fact_daily_price, fact_daily_price[VOLUME] * fact_daily_price[CLOSE])
	```

- Total Volume: Sum of traded volume.
	```DAX
	Total Volume = SUM(fact_daily_price[VOLUME])
	```

- Volume Trend (7D): 7-day average of total volume.
	```DAX
	Volume Trend (7D) =
			AVERAGEX(
					DATESINPERIOD(dim_date[CAL_DATE], LASTDATE(dim_date[CAL_DATE]), -7, DAY),
					[Total Volume]
			)
	```
