#!/usr/bin/env python3
import datetime as dt
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import msgspec


class Option(msgspec.Struct):
    ask: float
    askSize: int
    bid: float
    bidAskSize: str
    bidSize: int
    closePrice: float
    daysToExpiration: int
    deliverableNote: str
    delta: Union[float, str]
    description: str
    exchangeName: str
    exerciseType: str
    expirationDate: str
    expirationType: str
    extrinsicValue: Union[str, float]
    gamma: Union[float, str]
    high52Week: Union[float, str]
    highPrice: Union[float, str]
    inTheMoney: bool
    intrinsicValue: Union[float, str]
    iv_mark: Optional[float]  # USER ADDED
    last: Union[float, str]
    lastSize: int
    lastTradingDay: int
    low52Week: Union[float, str]
    lowPrice: Union[float, str]
    mark: Union[float, str]
    markChange: float
    markPercentChange: Union[float, str]
    mini: bool
    multiplier: float
    netChange: float
    nonStandard: bool
    openInterest: Union[int, str]
    openPrice: Union[float, str]
    optionDeliverablesList: List[Dict[str, Union[str, float]]]
    optionRoot: str
    pennyPilot: bool
    percentChange: float
    putCall: str
    q: float  # USER ADDED
    quoteTimeInLong: Union[int, str]
    r: Optional[float]  # USER ADDED
    rho: Union[float, str]
    settlementType: str
    strikePrice: Union[float, str]
    symbol: str
    t: float  # USER ADDED
    theta: Union[float, str]
    timeValue: float
    totalVolume: Union[int, str]
    tradeTimeInLong: Union[int, str]
    underlyingPrice: Union[float, str]  # USER ADDED
    vega: Union[float, str]

    @property
    def _header_keys(self) -> List[str]:
        return sorted(
            [
                "ask",
                "askSize",
                "bid",
                "bidAskSize",
                "bidSize",
                "closePrice",
                "daysToExpiration",
                "deliverableNote",
                "delta",
                "description",
                "exchangeName",
                "exerciseType",
                "expirationDate",
                "expirationType",
                "extrinsicValue",
                "gamma",
                "high52Week",
                "highPrice",
                "inTheMoney",
                "intrinsicValue",
                "iv_mark",
                "last",
                "lastSize",
                "lastTradingDay",
                "low52Week",
                "lowPrice",
                "mark",
                "markChange",
                "markPercentChange",
                "mini",
                "multiplier",
                "netChange",
                "nonStandard",
                "openInterest",
                "openPrice",
                "optionDeliverablesList",
                "optionRoot",
                "pennyPilot",
                "percentChange",
                "putCall",
                "q",
                "quoteTimeInLong",
                "r",
                "rho",
                "settlementType",
                "strikePrice",
                "symbol",
                "t",
                "theta",
                "timeValue",
                "totalVolume",
                "tradeTimeInLong",
                "underlyingPrice",
                "vega",
            ]
        )

    @property
    def _header(self) -> str:
        # return ",".join(list(map(self._mapper, self._header_keys)))
        return ",".join(self._header_keys)

    def _to_csv(self, oi: bool = False, sep: str = ",") -> str:
        """Get csv representation"""
        if oi:
            return sep.join(
                str(getattr(self, attr)).replace("NaN", "").replace("None", "")
                for attr in (
                    "symbol",
                    "strikePrice",
                    "putCall",
                    "expirationDate",
                    "daysToExpiration",
                    "openInterest",
                )
            )

        return sep.join(
            str(getattr(self, attr)).replace("NaN", "").replace("None", "")
            for attr in self._header_keys
        )

    def __repr__(self):  # sourcery skip: use-fstring-for-concatenation
        attrs = ",".join(
            # f"{attr}={getattr(self, attr)}"
            attr + "=" + str(getattr(self, attr)).replace("NaN", "").replace("None", "")
            for attr in self._header_keys
        )
        return f"{self.__class__.__name__}({attrs})"

    def __str__(self):
        return self.__repr__()

    # @classmethod
    # def fromcsv(cls, option: str):

    #     option_list = option.split(",")
    #     cols = [
    #         "ask",
    #         "bid",
    #         "closePrice",
    #         "daysToExpiration",
    #         "delta",
    #         "expirationDate",
    #         "gamma",
    #         "highPrice",
    #         "",  # "iv_last",
    #         "",  # "iv_mark",
    #         "last",
    #         "lowPrice",
    #         "mark",
    #         "markPercentChange",
    #         "openInterest",
    #         "openPrice",
    #         "putCall",
    #         "q",
    #         "quoteTimeInLong",
    #         "r",
    #         "rho",
    #         "strikePrice",
    #         "symbol",
    #         "theta",
    #         "totalVolume",
    #         "tradeTimeInLong",
    #         "underlyingPrice",
    #         "vega",
    #         "",  # "DtInserted"
    #     ]
    #     mapper = dict(zip(range(len(cols)), cols))
    #     _dict = {mapper[idx]: val for idx, val in enumerate(option_list) if mapper[idx]}
    #     return cls(_dict)  # type: ignore


class UnderlyingModel(msgspec.Struct):
    symbol: str
    description: str
    change: float
    percentChange: float
    close: float
    quoteTime: int
    tradeTime: int
    bid: float
    ask: float
    last: float
    mark: float
    markChange: float
    markPercentChange: float
    bidSize: int
    askSize: int
    highPrice: float
    lowPrice: float
    openPrice: float
    totalVolume: int
    exchangeName: str
    fiftyTwoWeekHigh: float
    fiftyTwoWeekLow: float
    delayed: bool


class OptionChainModel(msgspec.Struct):
    symbol: str
    status: str
    underlying: UnderlyingModel
    underlyingPrice: float
    strategy: str
    interval: float
    isDelayed: bool
    isIndex: bool
    interestRate: float
    volatility: float
    daysToExpiration: float
    numberOfContracts: int
    assetMainType: str
    assetSubType: str
    isChainTruncated: bool
    callExpDateMap: dict
    putExpDateMap: dict
    fetchTime: dt.datetime

    @property
    def expiries(self) -> List[str]:
        return list(self.callExpDateMap.keys())
