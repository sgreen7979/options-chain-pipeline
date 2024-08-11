#!/usr/bin/env python3
from typing import Dict
from typing import Optional
from typing import TypedDict


class BaseQuoteDict(TypedDict):
    assetMainType: str
    assetSubType: str
    ssid: int
    symbol: str


class BaseFundamentalDict(TypedDict):
    """
    "fundamental": {
    "avg10DaysVolume": 660263.0,
    "avg1YearVolume": 1024463.0,
    "divAmount": 5.2,
    "divFreq": 4,
    "divPayAmount": 1.3,
    "divYield": 4.45778,
    "eps": 0.54,
    "fundLeverageFactor": 0.0,
    "peRatio": 108.00547
    }
    """

    avg10DaysVolume: float
    avg1YearVolume: float
    divAmount: float
    divFreq: int
    divPayAmount: float
    divYield: float
    eps: float
    fundLeverageFactor: float
    peRatio: float


class FundamentalDict(BaseFundamentalDict, total=False):
    """
    "fundamental": {
    "avg10DaysVolume": 660263.0,
    "avg1YearVolume": 1024463.0,
    "declarationDate": "2024-06-03T04:00:00Z",
    "divAmount": 5.2,
    "divExDate": "2024-06-28T04:00:00Z",
    "divFreq": 4,
    "divPayAmount": 1.3,
    "divPayDate": "2024-07-15T04:00:00Z",
    "divYield": 4.45778,
    "eps": 0.54,
    "fundLeverageFactor": 0.0,
    "lastEarningsDate": "2024-04-22T04:00:00Z",
    "nextDivExDate": "2024-09-30T04:00:00Z",
    "nextDivPayDate": "2024-10-15T04:00:00Z",
    "peRatio": 108.00547
    }
    """

    declarationDate: str
    divExDate: str
    divPayDate: str
    lastEarningsDate: str
    nextDivExDate: str
    nextDivPayDate: str


class FundamentalQuoteResponse(BaseQuoteDict):
    fundamental: FundamentalDict


FundamentalDatabase = Dict[str, FundamentalQuoteResponse]
"""
    assetMainType: str
    assetSubType: str
    ssid: int
    symbol: str
    fundamental: FundamentalDict
    avg10DaysVolume: float
    avg1YearVolume: float
    declarationDate: Optional[str]
    divAmount: float
    divExDate: Optional[str]
    divFreq: int
    divPayAmount: float
    divPayDate: Optional[str]
    divYield: float
    eps: float
    fundLeverageFactor: float
    lastEarningsDate: Optional[str]
    nextDivExDate: Optional[str]
    nextDivPayDate: Optional[str]
    peRatio: float
"""


class MetadataDict(TypedDict):
    """
    { "last_updated": "2024-07-13T18:39:15.720834" }

    OR

    { "last_updated": null }
    """

    last_updated: Optional[str]
