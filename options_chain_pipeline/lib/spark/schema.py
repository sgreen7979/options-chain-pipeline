from pyspark.sql.types import (  # noqa
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    VarcharType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

__all__ = [
    # "OPTION_SCHEMA_IN",
    "OptionsChainSchema",
    # "EXTRA_SCHEMA",
    # "OPTION_SCHEMA_OUT",
    # "OptionsDataSchema",
    # "OptionsDataSchema1",
    "OptionsDataOutSchema",
    # "HEADERS_SCHEMA",
    "OptionsDataInSchema",
]


OptionsDataInSchema = StructType(
    [
        StructField("ask", DecimalType(10, 2), True),
        StructField("askSize", IntegerType(), True),
        StructField("bid", DecimalType(10, 2), True),
        StructField("bidAskSize", StringType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("closePrice", DecimalType(10, 2), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("delta", DecimalType(10, 2), True),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField("expirationDate", StringType(), True),
        StructField("expirationType", StringType(), True),
        StructField("extrinsicValue", DecimalType(10, 2), True),
        StructField("gamma", DecimalType(10, 2), True),
        StructField("high52Week", DecimalType(10, 2), True),
        StructField("highPrice", DecimalType(10, 2), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("intrinsicValue", DecimalType(10, 2), True),
        StructField("last", DecimalType(10, 2), True),
        StructField("lastSize", IntegerType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("low52Week", DecimalType(10, 2), True),
        StructField("lowPrice", DecimalType(10, 2), True),
        StructField("mark", DecimalType(10, 2), True),
        StructField("markChange", DecimalType(10, 2), True),
        StructField("markPercentChange", DecimalType(10, 2), True),
        StructField("mini", BooleanType(), True),
        StructField("multiplier", DecimalType(10, 2), True),
        StructField("netChange", DecimalType(10, 2), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("openInterest", LongType(), True),
        StructField("openPrice", DecimalType(10, 2), True),
        StructField(
            "optionDeliverablesList",
            ArrayType(
                StructType(
                    [
                        StructField("symbol", StringType(), True),
                        StructField("assetType", StringType(), True),
                        StructField("deliverableUnits", DoubleType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("optionRoot", StringType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("percentChange", DecimalType(10, 2), True),
        StructField("putCall", StringType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("rho", DecimalType(10, 2), True),
        StructField("settlementType", StringType(), True),
        StructField("strikePrice", DecimalType(10, 2), True),
        StructField("symbol", StringType(), False),
        StructField("theoreticalOptionValue", DecimalType(10, 2), True),
        StructField("theoreticalVolatility", DecimalType(10, 2), True),
        StructField("theta", DecimalType(10, 2), True),
        StructField("timeValue", DecimalType(10, 2), True),
        StructField("totalVolume", LongType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("vega", DecimalType(10, 2), True),
        StructField("volatility", DecimalType(10, 2), True),
    ]
)
"""

```python

OptionsDataInSchema = StructType([
    StructField("ask", DecimalType(10, 2), True),
    StructField("askSize", IntegerType(), True),
    StructField("bid", DecimalType(10, 2), True),
    StructField("bidAskSize", StringType(), True),
    StructField("bidSize", IntegerType(), True),
    StructField("closePrice", DecimalType(10, 2), True),
    StructField("daysToExpiration", IntegerType(), True),
    StructField("deliverableNote", StringType(), True),
    StructField("delta", DecimalType(10, 2), True),
    StructField("description", StringType(), True),
    StructField("exchangeName", StringType(), True),
    StructField("exerciseType", StringType(), True),
    StructField("expirationDate", StringType(), True),
    StructField("expirationType", StringType(), True),
    StructField("extrinsicValue", DecimalType(10, 2), True),
    StructField("gamma", DecimalType(10, 2), True),
    StructField("high52Week", DecimalType(10, 2), True),
    StructField("highPrice", DecimalType(10, 2), True),
    StructField("inTheMoney", BooleanType(), True),
    StructField("intrinsicValue", DecimalType(10, 2), True),
    StructField("last", DecimalType(10, 2), True),
    StructField("lastSize", IntegerType(), True),
    StructField("lastTradingDay", LongType(), True),
    StructField("low52Week", DecimalType(10, 2), True),
    StructField("lowPrice", DecimalType(10, 2), True),
    StructField("mark", DecimalType(10, 2), True),
    StructField("markChange", DecimalType(10, 2), True),
    StructField("markPercentChange", DecimalType(10, 2), True),
    StructField("mini", BooleanType(), True),
    StructField("multiplier", DecimalType(10, 2), True),
    StructField("netChange", DecimalType(10, 2), True),
    StructField("nonStandard", BooleanType(), True),
    StructField("openInterest", LongType(), True),
    StructField("openPrice", DecimalType(10, 2), True),
    StructField(
        "optionDeliverablesList",
        ArrayType(
            StructType([
                StructField("symbol", StringType(), True),
                StructField("assetType", StringType(), True),
                StructField("deliverableUnits", DoubleType(), True),
            ])
        ),
        True,
    ),
    StructField("optionRoot", StringType(), True),
    StructField("pennyPilot", BooleanType(), True),
    StructField("percentChange", DecimalType(10, 2), True),
    StructField("putCall", StringType(), True),
    StructField("quoteTimeInLong", LongType(), True),
    StructField("rho", DecimalType(10, 2), True),
    StructField("settlementType", StringType(), True),
    StructField("strikePrice", DecimalType(10, 2), True),
    StructField("symbol", StringType(), False),
    StructField("theoreticalOptionValue", DecimalType(10, 2), True),
    StructField("theoreticalVolatility", DecimalType(10, 2), True),
    StructField("theta", DecimalType(10, 2), True),
    StructField("timeValue", DecimalType(10, 2), True),
    StructField("totalVolume", LongType(), True),
    StructField("tradeTimeInLong", LongType(), True),
    StructField("vega", DecimalType(10, 2), True),
    StructField("volatility", DecimalType(10, 2), True),
])
```

"""


# Define schema for the options chain
OptionsChainSchema = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "underlying",
            StructType(
                [
                    StructField("symbol", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("change", DoubleType(), True),
                    StructField("percentChange", DoubleType(), True),
                    StructField("close", DoubleType(), True),
                    StructField("quoteTime", LongType(), True),
                    StructField("tradeTime", LongType(), True),
                    StructField("bid", DoubleType(), True),
                    StructField("ask", DoubleType(), True),
                    StructField("last", DoubleType(), True),
                    StructField("mark", DoubleType(), True),
                    StructField("markChange", DoubleType(), True),
                    StructField("markPercentChange", DoubleType(), True),
                    StructField("bidSize", IntegerType(), True),
                    StructField("askSize", IntegerType(), True),
                    StructField("highPrice", DoubleType(), True),
                    StructField("lowPrice", DoubleType(), True),
                    StructField("openPrice", DoubleType(), True),
                    StructField("totalVolume", IntegerType(), True),
                    StructField("exchangeName", StringType(), True),
                    StructField("fiftyTwoWeekHigh", DoubleType(), True),
                    StructField("fiftyTwoWeekLow", DoubleType(), True),
                    StructField("delayed", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField("strategy", StringType(), True),
        StructField("interval", DoubleType(), True),
        StructField("isDelayed", BooleanType(), True),
        StructField("isIndex", BooleanType(), True),
        StructField("interestRate", DoubleType(), True),
        StructField("underlyingPrice", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("daysToExpiration", DoubleType(), True),
        StructField("numberOfContracts", IntegerType(), True),
        StructField("assetMainType", StringType(), True),
        StructField("assetSubType", StringType(), True),
        StructField("isChainTruncated", BooleanType(), True),
        StructField(
            "callExpDateMap",
            MapType(
                StringType(), MapType(StringType(), ArrayType(OptionsDataInSchema))
            ),
            True,
        ),
        StructField(
            "putExpDateMap",
            MapType(
                StringType(), MapType(StringType(), ArrayType(OptionsDataInSchema))
            ),
            True,
        ),
        StructField("fetchTime", TimestampType(), True),
        StructField("dividendAmount", DoubleType(), True),
    ]
)
"""
```python
StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "underlying",
            StructType([
                StructField("symbol", StringType(), True),
                StructField("description", StringType(), True),
                StructField("change", DoubleType(), True),
                StructField("percentChange", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("quoteTime", LongType(), True),
                StructField("tradeTime", LongType(), True),
                StructField("bid", DoubleType(), True),
                StructField("ask", DoubleType(), True),
                StructField("last", DoubleType(), True),
                StructField("mark", DoubleType(), True),
                StructField("markChange", DoubleType(), True),
                StructField("markPercentChange", DoubleType(), True),
                StructField("bidSize", IntegerType(), True),
                StructField("askSize", IntegerType(), True),
                StructField("highPrice", DoubleType(), True),
                StructField("lowPrice", DoubleType(), True),
                StructField("openPrice", DoubleType(), True),
                StructField("totalVolume", IntegerType(), True),
                StructField("exchangeName", StringType(), True),
                StructField("fiftyTwoWeekHigh", DoubleType(), True),
                StructField("fiftyTwoWeekLow", DoubleType(), True),
                StructField("delayed", BooleanType(), True),
            ]),
            True,
        ),
        StructField("strategy", StringType(), True),
        StructField("interval", DoubleType(), True),
        StructField("isDelayed", BooleanType(), True),
        StructField("isIndex", BooleanType(), True),
        StructField("interestRate", DoubleType(), True),
        StructField("underlyingPrice", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("daysToExpiration", DoubleType(), True),
        StructField("numberOfContracts", IntegerType(), True),
        StructField("assetMainType", StringType(), True),
        StructField("assetSubType", StringType(), True),
        StructField("isChainTruncated", BooleanType(), True),
        StructField(
            "callExpDateMap",
            MapType(
                StringType(),
                MapType(
                    StringType(),
                    ArrayType(OPTION_SCHEMA_IN)
                )
            ),
            True,
        ),
        StructField(
            "putExpDateMap",
            MapType(
                StringType(),
                MapType(
                    StringType(),
                    ArrayType(OPTION_SCHEMA_IN)
                )
            ),
            True,
        ),
        StructField("fetchTime", StringType(), True),
    ]
)
```

"""


OptionsDataOutSchema = StructType(
    [
        StructField("ask", DecimalType(10, 2), True),
        StructField("askSize", IntegerType(), True),
        StructField("bid", DecimalType(10, 2), True),
        StructField("bidAskSize", StringType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("closePrice", DecimalType(10, 2), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("delta", DecimalType(10, 2), True),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField(
            "expirationDate", StringType(), True
        ),  # Will handle conversion separately
        StructField("expirationType", StringType(), True),
        StructField("extrinsicValue", DecimalType(10, 2), True),
        StructField("gamma", DecimalType(10, 2), True),
        StructField("high52Week", DecimalType(10, 2), True),
        StructField("highPrice", DecimalType(10, 2), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("intrinsicValue", DecimalType(10, 2), True),
        StructField("last", DecimalType(10, 2), True),
        StructField("lastSize", IntegerType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("low52Week", DecimalType(10, 2), True),
        StructField("lowPrice", DecimalType(10, 2), True),
        StructField("mark", DecimalType(10, 2), True),
        StructField("markChange", DecimalType(10, 2), True),
        StructField("markPercentChange", DecimalType(10, 2), True),
        StructField("mini", BooleanType(), True),
        StructField("multiplier", DecimalType(10, 2), True),
        StructField("netChange", DecimalType(10, 2), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("openInterest", LongType(), True),
        StructField("openPrice", DecimalType(10, 2), True),
        StructField("optionRoot", StringType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("percentChange", DecimalType(10, 2), True),
        StructField("putCall", StringType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("rho", DecimalType(10, 2), True),
        StructField("settlementType", StringType(), True),
        StructField("strikePrice", DecimalType(10, 2), True),
        StructField("symbol", StringType(), False),
        StructField("theoreticalOptionValue", DecimalType(10, 2), True),
        StructField("theoreticalVolatility", DecimalType(10, 2), True),
        StructField("theta", DecimalType(10, 2), True),
        StructField("timeValue", DecimalType(10, 2), True),
        StructField("totalVolume", LongType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("vega", DecimalType(10, 2), True),
        StructField("volatility", DecimalType(10, 2), True),
        StructField("underlyingPrice", DecimalType(10, 2), True),
        StructField("fetchTime", TimestampType(), False),
        StructField("iv_mark", FloatType(), True),
        StructField("extra", StringType(), True),
        StructField("ticker", StringType(), False),
    ]
)


################################################################################################
################################################################################################
################################################################################################
################################################################################################
################################################################################################
################################################################################################
# OLD
# ---------


# Define schema for individual options
OPTION_SCHEMA_IN = StructType(
    [
        StructField("putCall", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("last", DoubleType(), True),
        StructField("mark", DoubleType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("askSize", IntegerType(), True),
        StructField("bidAskSize", StringType(), True),
        StructField("lastSize", IntegerType(), True),
        StructField("highPrice", DoubleType(), True),
        StructField("lowPrice", DoubleType(), True),
        StructField("openPrice", DoubleType(), True),
        StructField("closePrice", DoubleType(), True),
        StructField("totalVolume", IntegerType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("netChange", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("delta", DoubleType(), True),
        StructField("gamma", DoubleType(), True),
        StructField("theta", DoubleType(), True),
        StructField("vega", DoubleType(), True),
        StructField("rho", DoubleType(), True),
        StructField("openInterest", IntegerType(), True),
        StructField("timeValue", DoubleType(), True),
        StructField("theoreticalOptionValue", DoubleType(), True),
        StructField("theoreticalVolatility", DoubleType(), True),
        StructField(
            "optionDeliverablesList",
            ArrayType(
                StructType(
                    [
                        StructField("symbol", StringType(), True),
                        StructField("assetType", StringType(), True),
                        StructField("deliverableUnits", DoubleType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("strikePrice", DoubleType(), True),
        StructField("expirationDate", StringType(), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("expirationType", StringType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("multiplier", DoubleType(), True),
        StructField("settlementType", StringType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("percentChange", DoubleType(), True),
        StructField("markChange", DoubleType(), True),
        StructField("markPercentChange", DoubleType(), True),
        StructField("intrinsicValue", DoubleType(), True),
        StructField("extrinsicValue", DoubleType(), True),
        StructField("optionRoot", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField("high52Week", DoubleType(), True),
        StructField("low52Week", DoubleType(), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("mini", BooleanType(), True),
    ]
)
"""
```python

StructType(
    [
        StructField("putCall", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("last", DoubleType(), True),
        StructField("mark", DoubleType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("askSize", IntegerType(), True),
        StructField("bidAskSize", StringType(), True),
        StructField("lastSize", IntegerType(), True),
        StructField("highPrice", DoubleType(), True),
        StructField("lowPrice", DoubleType(), True),
        StructField("openPrice", DoubleType(), True),
        StructField("closePrice", DoubleType(), True),
        StructField("totalVolume", IntegerType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("netChange", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("delta", DoubleType(), True),
        StructField("gamma", DoubleType(), True),
        StructField("theta", DoubleType(), True),
        StructField("vega", DoubleType(), True),
        StructField("rho", DoubleType(), True),
        StructField("openInterest", IntegerType(), True),
        StructField("timeValue", DoubleType(), True),
        StructField("theoreticalOptionValue", DoubleType(), True),
        StructField("theoreticalVolatility", DoubleType(), True),
        StructField(
            "optionDeliverablesList",
            ArrayType(StructType([
                StructField("symbol", StringType(), True),
                StructField("assetType", StringType(), True),
                StructField("deliverableUnits", DoubleType(), True),
            ])),
            True,
        ),
        StructField("strikePrice", DoubleType(), True),
        StructField("expirationDate", StringType(), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("expirationType", StringType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("multiplier", DoubleType(), True),
        StructField("settlementType", StringType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("percentChange", DoubleType(), True),
        StructField("markChange", DoubleType(), True),
        StructField("markPercentChange", DoubleType(), True),
        StructField("intrinsicValue", DoubleType(), True),
        StructField("extrinsicValue", DoubleType(), True),
        StructField("optionRoot", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField("high52Week", DoubleType(), True),
        StructField("low52Week", DoubleType(), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("mini", BooleanType(), True),
    ]
```

"""

EXTRA_SCHEMA = StructType(
    [
        # StructField("underlyingPrice", DoubleType(), True),
        # StructField("fetchTime", StringType(), True),
        StructField("q", DoubleType(), True),
        StructField("t", DoubleType(), True),
        StructField("r", DoubleType(), True),
        # StructField("iv_mark", DoubleType(), True),
    ]
)
"""
```python

StructType([
    StructField("q", DoubleType(), True),
    StructField("t", DoubleType(), True),
    StructField("r", DoubleType(), True),
    # StructField("iv_mark", DoubleType(), True),
])

```
"""


# Define schema for individual options
OPTION_SCHEMA_OUT = StructType(
    [
        StructField("putCall", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("last", DoubleType(), True),
        StructField("mark", DoubleType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("askSize", IntegerType(), True),
        StructField("bidAskSize", StringType(), True),
        StructField("lastSize", IntegerType(), True),
        StructField("highPrice", DoubleType(), True),
        StructField("lowPrice", DoubleType(), True),
        StructField("openPrice", DoubleType(), True),
        StructField("closePrice", DoubleType(), True),
        StructField("totalVolume", IntegerType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("netChange", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("delta", DoubleType(), True),
        StructField("gamma", DoubleType(), True),
        StructField("theta", DoubleType(), True),
        StructField("vega", DoubleType(), True),
        StructField("rho", DoubleType(), True),
        StructField("openInterest", IntegerType(), True),
        StructField("timeValue", DoubleType(), True),
        StructField("theoreticalOptionValue", DoubleType(), True),
        StructField("theoreticalVolatility", DoubleType(), True),
        StructField(
            "optionDeliverablesList",
            ArrayType(
                StructType(
                    [
                        StructField("symbol", StringType(), True),
                        StructField("assetType", StringType(), True),
                        StructField("deliverableUnits", DoubleType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("strikePrice", DoubleType(), True),
        StructField("expirationDate", StringType(), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("expirationType", StringType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("multiplier", DoubleType(), True),
        StructField("settlementType", StringType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("percentChange", DoubleType(), True),
        StructField("markChange", DoubleType(), True),
        StructField("markPercentChange", DoubleType(), True),
        StructField("intrinsicValue", DoubleType(), True),
        StructField("extrinsicValue", DoubleType(), True),
        StructField("optionRoot", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField("high52Week", DoubleType(), True),
        StructField("low52Week", DoubleType(), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("mini", BooleanType(), True),
        # New columns added in process_row, in this order!
        StructField("underlyingPrice", DoubleType(), True),
        StructField("fetchTime", StringType(), True),
        # StructField("q", DoubleType(), True),
        # StructField("t", DoubleType(), True),
        # StructField("r", DoubleType(), True),
        StructField("iv_mark", DoubleType(), True),
        StructField("extra", EXTRA_SCHEMA, True),
        StructField("ticker", StringType(), True),
    ]
)
"""
```python

StructType([
    StructField("putCall", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("description", StringType(), True),
    StructField("exchangeName", StringType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("last", DoubleType(), True),
    StructField("mark", DoubleType(), True),
    StructField("bidSize", IntegerType(), True),
    StructField("askSize", IntegerType(), True),
    StructField("bidAskSize", StringType(), True),
    StructField("lastSize", IntegerType(), True),
    StructField("highPrice", DoubleType(), True),
    StructField("lowPrice", DoubleType(), True),
    StructField("openPrice", DoubleType(), True),
    StructField("closePrice", DoubleType(), True),
    StructField("totalVolume", IntegerType(), True),
    StructField("tradeTimeInLong", LongType(), True),
    StructField("quoteTimeInLong", LongType(), True),
    StructField("netChange", DoubleType(), True),
    StructField("volatility", DoubleType(), True),
    StructField("delta", DoubleType(), True),
    StructField("gamma", DoubleType(), True),
    StructField("theta", DoubleType(), True),
    StructField("vega", DoubleType(), True),
    StructField("rho", DoubleType(), True),
    StructField("openInterest", IntegerType(), True),
    StructField("timeValue", DoubleType(), True),
    StructField("theoreticalOptionValue", DoubleType(), True),
    StructField("theoreticalVolatility", DoubleType(), True),
    StructField(
        "optionDeliverablesList",
        ArrayType(
            StructType([
                StructField("symbol", StringType(), True),
                StructField("assetType", StringType(), True),
                StructField("deliverableUnits", DoubleType(), True),
            ])
        ),
        True,
    ),
    StructField("strikePrice", DoubleType(), True),
    StructField("expirationDate", StringType(), True),
    StructField("daysToExpiration", IntegerType(), True),
    StructField("expirationType", StringType(), True),
    StructField("lastTradingDay", LongType(), True),
    StructField("multiplier", DoubleType(), True),
    StructField("settlementType", StringType(), True),
    StructField("deliverableNote", StringType(), True),
    StructField("percentChange", DoubleType(), True),
    StructField("markChange", DoubleType(), True),
    StructField("markPercentChange", DoubleType(), True),
    StructField("intrinsicValue", DoubleType(), True),
    StructField("extrinsicValue", DoubleType(), True),
    StructField("optionRoot", StringType(), True),
    StructField("exerciseType", StringType(), True),
    StructField("high52Week", DoubleType(), True),
    StructField("low52Week", DoubleType(), True),
    StructField("nonStandard", BooleanType(), True),
    StructField("pennyPilot", BooleanType(), True),
    StructField("inTheMoney", BooleanType(), True),
    StructField("mini", BooleanType(), True),
    # New columns added in process_row, in this order!
    StructField("underlyingPrice", DoubleType(), True),
    StructField("fetchTime", StringType(), True),
    StructField(
        "extra",
        StructType([
            StructField("q", DoubleType(), True),
            StructField("t", DoubleType(), True),
            StructField("r", DoubleType(), True),
            StructField("iv_mark", DoubleType(), True),
        ]),
        True
    ),
    StructField("ticker", StringType(), True),
])

```
"""
# fmt: off
OptionsDataSchema = StructType(
    [                                                                   # ord_sql   ord_spark
        StructField("putCall", StringType(), True),                     # 41        1
        StructField("symbol", StringType(), True),                      # 46        2
        StructField("description", StringType(), True),                 # 12        3
        StructField("exchangeName", StringType(), True),                # 13        4
        StructField("bid", DoubleType(), True),                         # 5         5
        StructField("ask", DoubleType(), True),                         # 3         6
        StructField("last", DoubleType(), True),                        # 24        7
        StructField("mark", DoubleType(), True),                        # 29        8
        StructField("bidSize", IntegerType(), True),                    # 7         9
        StructField("askSize", IntegerType(), True),                    # 4         10
        StructField("bidAskSize", StringType(), True),                  # 6         11
        StructField("lastSize", IntegerType(), True),                   # 25        12
        StructField("highPrice", DoubleType(), True),                   # 20        13
        StructField("lowPrice", DoubleType(), True),                    # 28        14
        StructField("openPrice", DoubleType(), True),                   # 37        15
        StructField("closePrice", DoubleType(), True),                  # 8         16
        StructField("totalVolume", IntegerType(), True),                # 51        17
        StructField("tradeTimeInLong", LongType(), True),               # 52        18
        StructField("quoteTimeInLong", LongType(), True),               # 42        19
        StructField("netChange", DoubleType(), True),                   # 34        20
        StructField("volatility", DoubleType(), True),                  # 55        21
        StructField("delta", DoubleType(), True),                       # 11        22
        StructField("gamma", DoubleType(), True),                       # 18        23
        StructField("theta", DoubleType(), True),                       # 49        24
        StructField("vega", DoubleType(), True),                        # 54        25
        StructField("rho", DoubleType(), True),                         # 43        26
        StructField("openInterest", IntegerType(), True),               # 36        27
        StructField("timeValue", DoubleType(), True),                   # 50        28
        StructField("theoreticalOptionValue", DoubleType(), True),      # 47        29
        StructField("theoreticalVolatility", DoubleType(), True),       # 48        30
        StructField("strikePrice", DoubleType(), True),                 # 45        31
        StructField("expirationDate", StringType(), True),              # 15        32
        StructField("daysToExpiration", IntegerType(), True),           # 9         33
        StructField("expirationType", StringType(), True),              # 16        34
        StructField("lastTradingDay", LongType(), True),                # 26        35
        StructField("multiplier", DoubleType(), True),                  # 33        36
        StructField("settlementType", StringType(), True),              # 44        37
        StructField("deliverableNote", StringType(), True),             # 10        38
        StructField("percentChange", DoubleType(), True),               # 40        39
        StructField("markChange", DoubleType(), True),                  # 30        40
        StructField("markPercentChange", DoubleType(), True),           # 31        41
        StructField("intrinsicValue", DoubleType(), True),              # 22        42
        StructField("extrinsicValue", DoubleType(), True),              # 17        43
        StructField("optionRoot", StringType(), True),                  # 38        44
        StructField("exerciseType", StringType(), True),                # 14        45
        StructField("high52Week", DoubleType(), True),                  # 19        46
        StructField("low52Week", DoubleType(), True),                   # 27        47
        StructField("nonStandard", BooleanType(), True),                # 35        48
        StructField("pennyPilot", BooleanType(), True),                 # 39        49
        StructField("inTheMoney", BooleanType(), True),                 # 21        50
        StructField("mini", BooleanType(), True),                       # 32        51
        StructField("underlyingPrice", DoubleType(), True),             # 53        52
        StructField("fetchTime", StringType(), True),                   # 2         53
        StructField("iv_mark", DoubleType(), True),                     # 23        54
        StructField("extra", EXTRA_SCHEMA, True),                       # 56        55
        StructField("ticker", StringType(), True),                      # 1         56
    ]
)
"""
```python

StructType([
    StructField("putCall", StringType(), True),                     # 41        1
    StructField("symbol", StringType(), True),                      # 46        2
    StructField("description", StringType(), True),                 # 12        3
    StructField("exchangeName", StringType(), True),                # 13        4
    StructField("bid", DoubleType(), True),                         # 5         5
    StructField("ask", DoubleType(), True),                         # 3         6
    StructField("last", DoubleType(), True),                        # 24        7
    StructField("mark", DoubleType(), True),                        # 29        8
    StructField("bidSize", IntegerType(), True),                    # 7         9
    StructField("askSize", IntegerType(), True),                    # 4         10
    StructField("bidAskSize", StringType(), True),                  # 6         11
    StructField("lastSize", IntegerType(), True),                   # 25        12
    StructField("highPrice", DoubleType(), True),                   # 20        13
    StructField("lowPrice", DoubleType(), True),                    # 28        14
    StructField("openPrice", DoubleType(), True),                   # 37        15
    StructField("closePrice", DoubleType(), True),                  # 8         16
    StructField("totalVolume", IntegerType(), True),                # 51        17
    StructField("tradeTimeInLong", LongType(), True),               # 52        18
    StructField("quoteTimeInLong", LongType(), True),               # 42        19
    StructField("netChange", DoubleType(), True),                   # 34        20
    StructField("volatility", DoubleType(), True),                  # 55        21
    StructField("delta", DoubleType(), True),                       # 11        22
    StructField("gamma", DoubleType(), True),                       # 18        23
    StructField("theta", DoubleType(), True),                       # 49        24
    StructField("vega", DoubleType(), True),                        # 54        25
    StructField("rho", DoubleType(), True),                         # 43        26
    StructField("openInterest", IntegerType(), True),               # 36        27
    StructField("timeValue", DoubleType(), True),                   # 50        28
    StructField("theoreticalOptionValue", DoubleType(), True),      # 47        29
    StructField("theoreticalVolatility", DoubleType(), True),       # 48        30
    StructField("strikePrice", DoubleType(), True),                 # 45        31
    StructField("expirationDate", StringType(), True),              # 15        32
    StructField("daysToExpiration", IntegerType(), True),           # 9         33
    StructField("expirationType", StringType(), True),              # 16        34
    StructField("lastTradingDay", LongType(), True),                # 26        35
    StructField("multiplier", DoubleType(), True),                  # 33        36
    StructField("settlementType", StringType(), True),              # 44        37
    StructField("deliverableNote", StringType(), True),             # 10        38
    StructField("percentChange", DoubleType(), True),               # 40        39
    StructField("markChange", DoubleType(), True),                  # 30        40
    StructField("markPercentChange", DoubleType(), True),           # 31        41
    StructField("intrinsicValue", DoubleType(), True),              # 22        42
    StructField("extrinsicValue", DoubleType(), True),              # 17        43
    StructField("optionRoot", StringType(), True),                  # 38        44
    StructField("exerciseType", StringType(), True),                # 14        45
    StructField("high52Week", DoubleType(), True),                  # 19        46
    StructField("low52Week", DoubleType(), True),                   # 27        47
    StructField("nonStandard", BooleanType(), True),                # 35        48
    StructField("pennyPilot", BooleanType(), True),                 # 39        49
    StructField("inTheMoney", BooleanType(), True),                 # 21        50
    StructField("mini", BooleanType(), True),                       # 32        51
    StructField("underlyingPrice", DoubleType(), True),             # 53        52
    StructField("fetchTime", StringType(), True),                   # 2         53
    StructField("iv_mark", DoubleType(), True),                     # 23        54
    StructField("extra", StringType(), True),                       # 56        55
    StructField("ticker", StringType(), True),                      # 1         56
])

```
"""
# fmt: on


OptionsDataSchema1 = StructType(
    [
        StructField("putCall", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("description", StringType(), True),
        StructField("exchangeName", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("last", DoubleType(), True),
        StructField("mark", DoubleType(), True),
        StructField("bidSize", IntegerType(), True),
        StructField("askSize", IntegerType(), True),
        StructField("bidAskSize", StringType(), True),
        StructField("lastSize", IntegerType(), True),
        StructField("highPrice", DoubleType(), True),
        StructField("lowPrice", DoubleType(), True),
        StructField("openPrice", DoubleType(), True),
        StructField("closePrice", DoubleType(), True),
        StructField("totalVolume", IntegerType(), True),
        StructField("tradeTimeInLong", LongType(), True),
        StructField("quoteTimeInLong", LongType(), True),
        StructField("netChange", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        StructField("delta", DoubleType(), True),
        StructField("gamma", DoubleType(), True),
        StructField("theta", DoubleType(), True),
        StructField("vega", DoubleType(), True),
        StructField("rho", DoubleType(), True),
        StructField("openInterest", IntegerType(), True),
        StructField("timeValue", DoubleType(), True),
        StructField("theoreticalOptionValue", DoubleType(), True),
        StructField("theoreticalVolatility", DoubleType(), True),
        StructField("strikePrice", DoubleType(), True),
        # StructField("expirationDate", StringType(), True),
        StructField("expirationDate", TimestampType(), True),
        StructField("daysToExpiration", IntegerType(), True),
        StructField("expirationType", StringType(), True),
        StructField("lastTradingDay", LongType(), True),
        StructField("multiplier", DoubleType(), True),
        StructField("settlementType", StringType(), True),
        StructField("deliverableNote", StringType(), True),
        StructField("percentChange", DoubleType(), True),
        StructField("markChange", DoubleType(), True),
        StructField("markPercentChange", DoubleType(), True),
        StructField("intrinsicValue", DoubleType(), True),
        StructField("extrinsicValue", DoubleType(), True),
        StructField("optionRoot", StringType(), True),
        StructField("exerciseType", StringType(), True),
        StructField("high52Week", DoubleType(), True),
        StructField("low52Week", DoubleType(), True),
        StructField("nonStandard", BooleanType(), True),
        StructField("pennyPilot", BooleanType(), True),
        StructField("inTheMoney", BooleanType(), True),
        StructField("mini", BooleanType(), True),
        StructField("underlyingPrice", DoubleType(), True),
        # StructField("fetchTime", StringType(), False),
        StructField("fetchTime", TimestampNTZType(), False),
        StructField("iv_mark", DoubleType(), True),
        StructField("extra", StringType(), True),
        StructField("ticker", StringType(), False),
    ]
)


HEADERS_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("fetchTime", StringType(), False),
        StructField("divAmt", DoubleType(), False),
    ]
)
