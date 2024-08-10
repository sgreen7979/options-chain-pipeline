#!/usr/bin/env python3

import datetime
from typing import Literal
from typing import Optional
from typing import Union

import msgspec

# from daily.iv import civhalley
from daily.utils.mapper import Mapper


class Option(msgspec.Struct):
    # underlying: str
    ask: float
    bid: float
    closePrice: float
    daysToExpiration: int
    delta: Union[float, str]
    expirationDate: str
    gamma: Union[float, str]
    highPrice: Union[float, str]
    intrinsicValue: Union[float, str]
    last: Union[float, str]
    lowPrice: Union[float, str]
    mark: Union[float, str]
    markPercentChange: Union[float, str]
    openInterest: Union[int, str]
    openPrice: Union[float, str]
    putCall: Literal["CALL", "PUT"]
    quoteTimeInLong: Union[int, str]
    rho: Union[float, str]
    strikePrice: Union[float, str]
    symbol: str
    theta: Union[float, str]
    totalVolume: Union[int, str]
    tradeTimeInLong: Union[int, str]
    underlyingPrice: Union[float, str]
    vega: Union[float, str]
    q: float = 0.0
    r: float = 0.0
    iv_mark: Optional[float] = None

    # @property
    # def iv_mark(self) -> Union[float, None]:
    #     return civhalley(
    #         price=float(self.mark),
    #         putCall=self.putCall,
    #         S=float(self.underlyingPrice),
    #         K=float(self.strikePrice),
    #         dte=self.daysToExpiration,
    #         r=self.r,
    #         q=self.q,
    #     )

    @property
    def iv_last(self) -> None:
        return None

    @property
    def volume(self) -> int:
        return int(self.totalVolume)

    @property
    def expiry(self) -> datetime.date:
        return datetime.datetime.fromisoformat(self.expirationDate).date()
        # return datetime.datetime.fromtimestamp(self.expirationDate / 1000).date()

    @property
    def trade_time(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(self.tradeTimeInLong) / 1000)

    @property
    def quote_time(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(self.quoteTimeInLong) / 1000)

    @property
    def _primary_key(self) -> tuple[str, datetime.datetime]:
        """representation of primary key as perceived by sql"""
        return (self.symbol, self.trade_time)

    @property
    def _header_keys(self) -> list[str]:
        return sorted(
            [
                i
                for i in dir(self)
                if not i.startswith("_")
                and i
                not in (
                    "expirationDate",
                    "intrinsicValue",
                    "quoteTimeInLong",
                    "totalVolume",
                    "tradeTimeInLong",
                    "fromdict",
                    "fromcsv",
                    "fromlist",
                    "fromfile",
                )
            ]
        )

    @property
    def _header(self) -> str:
        return ",".join(list(map(self._mapper, self._header_keys)))

    def _to_csv(self, oi=False) -> str:
        """Get csv representation"""
        if oi:
            return ",".join(
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

        return ",".join(
            str(getattr(self, attr)).replace("NaN", "").replace("None", "")
            for attr in self._header_keys
        )

    @property
    def _mapper(self) -> Mapper:
        return Mapper(
            {
                "underlyingPrice": "underlyingP",
                "strikePrice": "strike",
                "expirationDate": "expiry",
                "daysToExpiration": "dte",
                "openInterest": "oi",
                "totalVolume": "volume",
                "highPrice": "high",
                "lowPrice": "low",
                "openPrice": "[open]",
                "closePrice": "[close]",
                "markPercentChange": "mark_perc_change",
            }
        )

    # def __getattribute__(self, attr):
    #     return str(getattr(self, attr)).replace("NaN", "").replace("None", "")

    def __repr__(self):  # sourcery skip: use-fstring-for-concatenation
        attrs = ",".join(
            # f"{attr}={getattr(self, attr)}"
            attr + "=" + str(getattr(self, attr)).replace("NaN", "").replace("None", "")
            for attr in self._header_keys
        )
        return f"{self.__class__.__name__}({attrs})"

    def __str__(self):
        return self.__repr__()

    @classmethod
    def fromcsv(cls, option: str):

        option_list = option.split(",")
        cols = [
            "ask",
            "bid",
            "closePrice",
            "daysToExpiration",
            "delta",
            "expirationDate",
            "gamma",
            "highPrice",
            "",  # "iv_last",
            "",  # "iv_mark",
            "last",
            "lowPrice",
            "mark",
            "markPercentChange",
            "openInterest",
            "openPrice",
            "putCall",
            "q",
            "quoteTimeInLong",
            "r",
            "rho",
            "strikePrice",
            "symbol",
            "theta",
            "totalVolume",
            "tradeTimeInLong",
            "underlyingPrice",
            "vega",
            "",  # "DtInserted"
        ]
        mapper = dict(zip(range(len(cols)), cols))
        _dict = {mapper[idx]: val for idx, val in enumerate(option_list) if mapper[idx]}
        return cls(_dict)  # type: ignore
