#!/usr/bin/env python3
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
    "OptionsChainSchema",
    "OptionsDataOutSchema",
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
