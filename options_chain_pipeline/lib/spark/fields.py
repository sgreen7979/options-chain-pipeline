from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
)

putCall = StructField("putCall", StringType(), True)
"""StringType()"""
symbol = StructField("symbol", StringType(), False)
"""StringType()\n
#### **non-nullable**
"""
description = StructField("description", StringType(), True)
"""StringType()"""
exchangeName = StructField("exchangeName", StringType(), True)
"""StringType()"""
bid = StructField("bid", DoubleType(), True)
"""DoubleType()"""
ask = StructField("ask", DoubleType(), True)
"""DoubleType()"""
last = StructField("last", DoubleType(), True)
"""DoubleType()"""
mark = StructField("mark", DoubleType(), True)
"""DoubleType()"""
bidSize = StructField("bidSize", IntegerType(), True)
"""IntegerType()"""
askSize = StructField("askSize", IntegerType(), True)
"""IntegerType()"""
bidAskSize = StructField("bidAskSize", StringType(), True)
"""StringType()"""
lastSize = StructField("lastSize", IntegerType(), True)
"""IntegerType()"""
highPrice = StructField("highPrice", DoubleType(), True)
"""DoubleType()"""
lowPrice = StructField("lowPrice", DoubleType(), True)
"""DoubleType()"""
openPrice = StructField("openPrice", DoubleType(), True)
"""DoubleType()"""
closePrice = StructField("closePrice", DoubleType(), True)
"""DoubleType()"""
totalVolume = StructField("totalVolume", IntegerType(), True)
"""IntegerType()"""
tradeTimeInLong = StructField("tradeTimeInLong", LongType(), True)
"""LongType()"""
quoteTimeInLong = StructField("quoteTimeInLong", LongType(), True)
"""LongType()"""
netChange = StructField("netChange", DoubleType(), True)
"""DoubleType()"""
volatility = StructField("volatility", DoubleType(), True)
"""DoubleType()"""
delta = StructField("delta", DoubleType(), True)
"""DoubleType()"""
gamma = StructField("gamma", DoubleType(), True)
"""DoubleType()"""
theta = StructField("theta", DoubleType(), True)
"""DoubleType()"""
vega = StructField("vega", DoubleType(), True)
"""DoubleType()"""
rho = StructField("rho", DoubleType(), True)
"""DoubleType()"""
openInterest = StructField("openInterest", IntegerType(), True)
"""IntegerType()"""
timeValue = StructField("timeValue", DoubleType(), True)
"""DoubleType()"""
theoreticalOptionValue = StructField("theoreticalOptionValue", DoubleType(), True)
"""DoubleType()"""
theoreticalVolatility = StructField("theoreticalVolatility", DoubleType(), True)
"""DoubleType()"""
strikePrice = StructField("strikePrice", DoubleType(), True)
"""DoubleType()"""
expirationDate = StructField("expirationDate", StringType(), True)
"""StringType()"""
daysToExpiration = StructField("daysToExpiration", IntegerType(), True)
"""IntegerType()"""
expirationType = StructField("expirationType", StringType(), True)
"""StringType()"""
lastTradingDay = StructField("lastTradingDay", LongType(), True)
"""LongType()"""
multiplier = StructField("multiplier", DoubleType(), True)
"""DoubleType()"""
settlementType = StructField("settlementType", StringType(), True)
"""StringType()"""
deliverableNote = StructField("deliverableNote", StringType(), True)
"""StringType()"""
percentChange = StructField("percentChange", DoubleType(), True)
"""DoubleType()"""
markChange = StructField("markChange", DoubleType(), True)
"""DoubleType()"""
markPercentChange = StructField("markPercentChange", DoubleType(), True)
"""DoubleType()"""
intrinsicValue = StructField("intrinsicValue", DoubleType(), True)
"""DoubleType()"""
extrinsicValue = StructField("extrinsicValue", DoubleType(), True)
"""DoubleType()"""
optionRoot = StructField("optionRoot", StringType(), True)
"""StringType()"""
exerciseType = StructField("exerciseType", StringType(), True)
"""StringType()"""
high52Week = StructField("high52Week", DoubleType(), True)
"""DoubleType()"""
low52Week = StructField("low52Week", DoubleType(), True)
"""DoubleType()"""
nonStandard = StructField("nonStandard", BooleanType(), True)
"""BooleanType()"""
pennyPilot = StructField("pennyPilot", BooleanType(), True)
"""BooleanType()"""
inTheMoney = StructField("inTheMoney", BooleanType(), True)
"""BooleanType()"""
mini = StructField("mini", BooleanType(), True)
"""BooleanType()"""
underlyingPrice = StructField("underlyingPrice", DoubleType(), True)
"""DoubleType()"""
fetchTime = StructField("fetchTime", StringType(), False)
"""StringType()\n
#### **non-nullable**
"""
iv_mark = StructField("iv_mark", DoubleType(), True)
"""DoubleType()"""
extra = StructField("extra", StringType(), True)
"""StringType()"""
ticker = StructField("ticker", StringType(), False)
"""StringType()\n
#### **non-nullable**
"""
