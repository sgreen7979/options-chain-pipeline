#!/usr/bin/env python3
import datetime as dt
from typing import Annotated
from typing import List
from typing import Literal
from typing import Tuple
from typing import Union

MarketDataType = List[Tuple[dt.date, float]]
DateValueMonthType = Union[dt.date, dt.datetime, str]
URLNameType = Literal[
    "BILL_RATES",
    "LT_RATES_AND_EXTRAPOLATION_FACTORS",
    "PAR_YIELD_CURVE_RATES",
    "REAL_LT_RATE_AVGS",
]
SvensonParamsType = Annotated[List[float], 6]
NelsonSiegelParamsType = Annotated[List[float], 4]
