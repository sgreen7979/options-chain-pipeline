#!/usr/bin/env python3

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
import datetime as dt
import json
import logging
from typing import Annotated
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import TypedDict
from typing import Union

import pandas as pd
import plotly.graph_objects as go

# from daily.types import StrPath
# from daily.fundamental.load import load_fundamentaldb
# from daily.options.chain_formats import ChainFormats
from daily.options.decoder import Decoder
from daily.options.encoder import Encoder

from daily.options.mcal.parallel import MarketCalendar
from daily.options.mcal.calendars import MarketCalendars

# from daily.options.models import Option

from daily.options.iv_engine import ImpliedVolatilityEngine as IVEngine
from daily.term_structure import SvenssonYieldTermStructure
from daily.rates import continuously_compounding_rate

# from daily.redis import Client as Redis

# from daily.term_structure import get_market_data


logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "[%(asctime)s][%(levelname)s][%(process)d][%(threadName)s]"
    "[%(filename)s][%(lineno)d]:%(message)s"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

# RED: Redis = Redis()

_cal_cache = {}

yts = None
ive = None


def get_yts():
    global yts
    if yts is None:
        yts = SvenssonYieldTermStructure.without_data()


def get_ive():
    global yts
    if yts is None:
        get_yts()
    assert isinstance(yts, SvenssonYieldTermStructure)  # mypy
    global ive
    # ive = IVEngine(yts)
    ive = IVEngine()


# yts = SvenssonYieldTermStructure.without_data()
# ive = IVEngine(yts)


def get_cal(market_name: str):
    if market_name not in ("NYSE", "NASDAQ"):
        market_name = "NYSE"
    global _cal_cache
    if market_name not in _cal_cache:
        market = MarketCalendars[market_name]
        _cal_cache[market_name] = MarketCalendar(market)
    return _cal_cache[market_name]


OptionDictType = Dict[str, Any]
OptionsType = List[OptionDictType]
StrikeEntryType = List[OptionDictType]
ExpiryDictType = Dict[str, StrikeEntryType]
ExpDateMapType = Dict[str, ExpiryDictType]


class optionDeliverablesDictType(TypedDict):
    """
    {
        "symbol": "<TICKER>",
        "assetType": "STOCK",
        "deliverableUnits": 100.0,
    }
    """

    symbol: str
    assetType: str
    deliverableUnits: float


class optionDictType(TypedDict):
    """
    {
        "putCall": "CALL",
        "symbol": "AAPL  240607C00110000",
        "description": "AAPL 06/07/2024 110.00 C",
        "exchangeName": "OPR",
        "bid": 86.05,
        "ask": 87.3,
        "last": 85.1,
        "mark": 86.68,
        "bidSize": 50,
        "askSize": 50,
        "bidAskSize": "50X50",
        "lastSize": 0,
        "highPrice": 0.0,
        "lowPrice": 0.0,
        "openPrice": 0.0,
        "closePrice": 84.51,
        "totalVolume": 0,
        "tradeTimeInLong": 1717526118572,
        "quoteTimeInLong": 1717790391106,
        "netChange": 0.59,
        "volatility": 1201.009,
        "delta": 1.0,
        "gamma": 0.0,
        "theta": -0.034,
        "vega": 0.0,
        "rho": 0.0,
        "openInterest": 0,
        "timeValue": -1.79,
        "theoreticalOptionValue": 86.924,
        "theoreticalVolatility": 29.0,
        "optionDeliverablesList": [
            {
                "symbol": "AAPL",
                "assetType": "STOCK",
                "deliverableUnits": 100.0
            }
        ],
        "strikePrice": 110.0,
        "expirationDate": "2024-06-07T20:00:00.000+00:00",
        "daysToExpiration": 0,
        "expirationType": "W",
        "lastTradingDay": 1717804800000,
        "multiplier": 100.0,
        "settlementType": "P",
        "deliverableNote": "100 AAPL",
        "percentChange": 0.7,
        "markChange": 2.17,
        "markPercentChange": 2.56,
        "intrinsicValue": 86.89,
        "extrinsicValue": -1.79,
        "optionRoot": "AAPL",
        "exerciseType": "A",
        "high52Week": 85.1,
        "low52Week": 74.46,
        "nonStandard": false,
        "pennyPilot": true,
        "inTheMoney": true,
        "mini": false
    }
    """

    putCall: str
    symbol: str
    description: str
    exchangeName: str
    bid: float
    ask: float
    last: float
    mark: float
    bidSize: int
    askSize: int
    bidAskSize: str
    lastSize: int
    highPrice: float
    lowPrice: float
    openPrice: float
    closePrice: float
    totalVolume: int
    tradeTimeInLong: int
    quoteTimeInLong: int
    netChange: float
    volatility: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    openInterest: int
    timeValue: float
    theoreticalOptionValue: float
    theoreticalVolatility: float
    optionDeliverablesList: List[optionDeliverablesDictType]
    strikePrice: float
    expirationDate: str
    daysToExpiration: int
    expirationType: str
    lastTradingDay: int
    multiplier: float
    settlementType: str
    deliverableNote: str
    percentChange: float
    markChange: float
    markPercentChange: float
    intrinsicValue: float
    extrinsicValue: float
    optionRoot: str
    exerciseType: str
    high52Week: float
    low52Week: float
    nonStandard: bool
    pennyPilot: bool
    inTheMoney: bool
    mini: bool


class extraDictType(TypedDict):
    """
    "extra": {
        "t": 0.01984126984126984,
        "q": 0.0,
        "r": 0.058097375394494694
    }
    """

    t: float
    q: float
    r: float


class calculatedOptionDictType(TypedDict):
    """
    {
        "putCall": "CALL",
        "symbol": "AAPL  240607C00110000",
        "description": "AAPL 06/07/2024 110.00 C",
        "exchangeName": "OPR",
        "bid": 86.05,
        "ask": 87.3,
        "last": 85.1,
        "mark": 86.68,
        "bidSize": 50,
        "askSize": 50,
        "bidAskSize": "50X50",
        "lastSize": 0,
        "highPrice": 0.0,
        "lowPrice": 0.0,
        "openPrice": 0.0,
        "closePrice": 84.51,
        "totalVolume": 0,
        "tradeTimeInLong": 1717526118572,
        "quoteTimeInLong": 1717790391106,
        "netChange": 0.59,
        "volatility": 1201.009,
        "delta": 1.0,
        "gamma": 0.0,
        "theta": -0.034,
        "vega": 0.0,
        "rho": 0.0,
        "openInterest": 0,
        "timeValue": -1.79,
        "theoreticalOptionValue": 86.924,
        "theoreticalVolatility": 29.0,
        "optionDeliverablesList": [
            {
                "symbol": "AAPL",
                "assetType": "STOCK",
                "deliverableUnits": 100.0
            }
        ],
        "strikePrice": 110.0,
        "expirationDate": "2024-06-07T20:00:00.000+00:00",
        "daysToExpiration": 0,
        "expirationType": "W",
        "lastTradingDay": 1717804800000,
        "multiplier": 100.0,
        "settlementType": "P",
        "deliverableNote": "100 AAPL",
        "percentChange": 0.7,
        "markChange": 2.17,
        "markPercentChange": 2.56,
        "intrinsicValue": 86.89,
        "extrinsicValue": -1.79,
        "optionRoot": "AAPL",
        "exerciseType": "A",
        "high52Week": 85.1,
        "low52Week": 74.46,
        "nonStandard": false,
        "pennyPilot": true,
        "inTheMoney": true,
        "mini": false,
        "iv_mark": 0.29351549626849227,
        "extra": {
            "t": 0.01984126984126984,
            "q": 0.0,
            "r": 0.058097375394494694
        }
    }
    """

    putCall: str
    symbol: str
    description: str
    exchangeName: str
    bid: float
    ask: float
    last: float
    mark: float
    bidSize: int
    askSize: int
    bidAskSize: str
    lastSize: int
    highPrice: float
    lowPrice: float
    openPrice: float
    closePrice: float
    totalVolume: int
    tradeTimeInLong: int
    quoteTimeInLong: int
    netChange: float
    volatility: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    openInterest: int
    timeValue: float
    theoreticalOptionValue: float
    theoreticalVolatility: float
    optionDeliverablesList: List[optionDeliverablesDictType]
    strikePrice: float
    expirationDate: str
    daysToExpiration: int
    expirationType: str
    lastTradingDay: int
    multiplier: float
    settlementType: str
    deliverableNote: str
    percentChange: float
    markChange: float
    markPercentChange: float
    intrinsicValue: float
    extrinsicValue: float
    optionRoot: str
    exerciseType: str
    high52Week: float
    low52Week: float
    nonStandard: bool
    pennyPilot: bool
    inTheMoney: bool
    mini: bool
    iv_mark: Optional[float]
    extra: Optional[extraDictType]


class underlyingInfo(TypedDict):
    """
    {
        "symbol": "AAPL",
        "description": "Apple Inc",
        "change": 2.41,
        "percentChange": 1.24,
        "close": 194.48,
        "quoteTime": 1717804799894,
        "tradeTime": 1717804798406,
        "bid": 196.85,
        "ask": 196.9,
        "last": 196.89,
        "mark": 196.89,
        "markChange": 2.41,
        "markPercentChange": 1.24,
        "bidSize": 1,
        "askSize": 4,
        "highPrice": 196.94,
        "lowPrice": 194.14,
        "openPrice": 194.65,
        "totalVolume": 53103912,
        "exchangeName": "NASDAQ",
        "fiftyTwoWeekHigh": 199.62,
        "fiftyTwoWeekLow": 164.08,
        "delayed": false
    }
    """

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


ApplyFunctionType = Callable[..., OptionDictType]
newApplyFunctionType = Callable[[optionDictType], calculatedOptionDictType]


class OptionChain:
    """Parses and manages option chain data from TD Ameritrade.

    Args:
        `data` (`dict`):
            JSON of response to get request from TD Ameritrade.

        `yield_curve` (`CubicSpline`, optional):
            Object utilized to derive the risk-free interest rate
            for individual options to calculate implied volatility.

        `dividend_amt` (`float`, optional):
            Last dividend amount.
            Defaults to None, in which case it will be fetched.

        `test_insert` (`bool`, optional):
            When true, insert data to a test sql db table.
            Defaults to False.

        `decoder` (`Decoder`, optional):
            Class that handles decoding individual options.
            Defaults to `Decoder`.

        `encoder` (`Encoder`, optional):
            Class that handles encoding individual options.
            Defaults to `Encoder`.
    """

    # SQL_DB = f"[{databases.stock_database}]"
    # SQL_SCHEMA = "[dbo]"
    # SQL_TEST_TABLE = "[option_rest_neww_arch_test]"
    # SQL_TEST_TABLE_OI = "[openinterest_test]"
    # SQL_TABLE = "[option_rest_neww_arch]"
    # SQL_TABLE_PK = databases.get_db_table(
    #     databases.stock_database, "dbo", "trigger_table"
    # )
    # SQL_TABLE_NO_PK = databases.get_db_table(
    #     databases.stock_database, "dbo", "staging_table"
    # )
    # SQL_TABLE_OI = f"[{databases.stock_database}].[dbo].[openinterest]"

    def __init__(
        self,
        data: dict,
        # yield_curve: Optional[SvenssonYieldTermStructure] = None,
        dividend_amt: Optional[float] = None,
        apply: Optional[ApplyFunctionType] = None,
        # test_insert: bool = False,
        decoder: Optional[Decoder] = None,
        encoder: Optional[Encoder] = None,
        # chain_format: str = ChainFormats.DEFAULT,
        **kwargs,
    ) -> None:

        self.chain = data
        # self.market_calendar = MarketCalendar()
        # ref_date, mkt_data = get_market_data()

        # self.iv_engine = IVEngine(
        #     # IVEngine.PriceEngines.BinomialTree(),
        #     IVEngine.YieldTermStructures.Svensson(ref_date, mkt_data),
        #     # MarketCalendar(),
        # )
        # self.market_calendar = MarketCalendar()

        # if yield_curve is None:
        #     # from daily.rates import spline

        #     # yield_curve = spline
        #     reference_date, market_data = get_market_data()
        #     yield_curve = SvenssonYieldTermStructure(reference_date, market_data)

        # self.yield_curve = yield_curve

        # self._dividend_amt = dividend_amt or self.get_dividend_amount()
        self._dividend_amt = dividend_amt or 0.0
        if apply is not None:
            self.market_calendar = get_cal(self.underlying["exchangeName"])
            get_ive()

        self.filtered: bool = False
        self._expiries: set = set()
        # self.test_insert = test_insert
        self.decoder = decoder
        self.encoder = encoder
        # self.chain_format = chain_format
        # self.date = kwargs.pop("date", "")
        # self.fetchTime: dt.datetime = self.chain.get("fetch_time", dt.datetime.now())
        self.fetchTime: str = self.chain["fetchTime"]
        self.fetchTimeDt: dt.datetime = dt.datetime.fromisoformat(
            self.chain["fetchTime"]
        )

        for key, val in kwargs.items():
            setattr(self, key, val)

        # self.apply = apply or self.prepare_dict
        self.apply = apply
        self.all = self.extract_options(self.apply)

    @property
    def decoder(self):
        return self._decoder

    @decoder.setter
    def decoder(self, decoder: Optional[Decoder]) -> None:
        self._decoder = decoder if decoder else Decoder()

    @property
    def encoder(self):
        return self._encoder

    @encoder.setter
    def encoder(self, encoder: Optional[Encoder]) -> None:
        self._encoder = encoder if encoder else Encoder()

    # @staticmethod
    # def validate(resp: requests.Response) -> bool:
    #     try:
    #         data = resp.json()
    #         return "status" in data and data["status"] == "SUCCESS"
    #     except json.JSONDecodeError:
    #         return False

    # @lru_cache
    # def get_dividend_amount(self) -> float:

    #     if dividend_amount := RED.hget(
    #         hashname="fundamental", key=self.underlying, default=None
    #     ):
    #         return float(dividend_amount)

    #     FUNDAMENTALS_DB = load_fundamentaldb(keys=["dividendAmount"], defaults=[0.0])

    #     return FUNDAMENTALS_DB.get(self.underlying, 0.0)  # type: ignore

    @property
    def underlying(self) -> underlyingInfo:
        return self.chain["underlying"]

    @property
    def expiries(self) -> List[str]:
        # return sorted(self._expiries, key=lambda e: int(e.split(":")[1]))
        return sorted(self._expiries, key=self._dte_from_expiry)

    def months(self) -> Dict[str, str]:
        data = {}
        for expiry in iter(self.expiries):
            month_str = self._expiry_to_month_str(expiry)
            data[expiry] = month_str
        return data

    def _expiry_to_month_str(self, expiry: str) -> str:
        return self._date_from_expiry(expiry).strftime("%b")

    def _expiry_to_month_int(self, expiry: str) -> int:
        return self._date_from_expiry(expiry).month

    @property
    def underlying_price(self) -> float:
        return self.chain["underlyingPrice"]

    @property
    def dividend_yield(self) -> float:
        if self._dividend_amt <= 0 or self.underlying_price <= 0.0001:
            return 0.0
        return float(continuously_compounding_rate(self._dividend_amt / self.underlying_price))

    # def calls(self) -> List[Dict[str, Any]]:

    #     def lazy(it):
    #         yield from it

    #     calls: List[Dict[str, Any]] = []
    #     for expiry in lazy(self.chain["callExpDateMap"]):
    #         self._expiries.add(expiry.split(':')[0])
    #         for strike in lazy(self.chain["callExpDateMap"][expiry]):
    #             for call in lazy(self.chain["callExpDateMap"][expiry][strike]):
    #                 calls.append(call)
    #         # calls.extend(
    #         #     self.chain["callExpDateMap"][expiry][strike][0]
    #         #     for strike in self.chain["callExpDateMap"][expiry].keys()
    #         # )
    #     return calls

    # def puts(self) -> List[Dict[str, Any]]:

    #     def lazy(it):
    #         yield from it

    #     puts: List[Dict[str, Any]] = []
    #     for expiry in lazy(self.chain["putExpDateMap"]):
    #         self._expiries.add(expiry.split(':')[0])
    #         for strike in lazy(self.chain["putExpDateMap"][expiry]):
    #             for put in lazy(self.chain["putExpDateMap"][expiry][strike]):
    #                 puts.append(put)
    #         # calls.extend(
    #         #     self.chain["callExpDateMap"][expiry][strike][0]
    #         #     for strike in self.chain["callExpDateMap"][expiry].keys()
    #         # )
    #     return puts

    # def _extract_options(self) -> List[Dict]:
    #     # def _extract_options(self) -> List[Option]:
    #     # _all = self.calls() + self.puts()
    #     # _all = list(map(self.add_global_fields, _all))
    #     # _all = [i for i in _all if i is not None]
    #     # return list(map(self.decoder.decode, _all))
    #     all_options = self.calls() + self.puts()
    #     return list(map(self.prepare_dict, all_options))

    @property
    def callExpDateMap(self) -> ExpDateMapType:
        return self.chain["callExpDateMap"]

    @property
    def putExpDateMap(self) -> ExpDateMapType:
        return self.chain["putExpDateMap"]

    def _extract_options(self) -> List[OptionDictType]:
        callExpDateMap = self.callExpDateMap
        options = []
        for exp in iter(callExpDateMap):
            self._expiries.add(exp)
            for strike in iter(callExpDateMap[exp]):
                for option in iter(callExpDateMap[exp][strike]):
                    options.append(option)

        putExpDateMap = self.putExpDateMap
        for exp in iter(putExpDateMap):
            for strike in iter(putExpDateMap[exp]):
                for option in iter(putExpDateMap[exp][strike]):
                    options.append(option)

        return options

    def walk(self) -> Generator[Tuple[str, str, OptionDictType], Any, None]:
        callExpDateMap = self.callExpDateMap
        for exp in iter(callExpDateMap):
            self._expiries.add(exp)
            for strike in iter(callExpDateMap[exp]):
                for option in iter(callExpDateMap[exp][strike]):
                    yield (exp, strike, option)

        putExpDateMap = self.putExpDateMap
        for exp in iter(putExpDateMap):
            for strike in iter(putExpDateMap[exp]):
                for option in iter(putExpDateMap[exp][strike]):
                    yield (exp, strike, option)

    def extract_options(
        self, apply: Optional[Callable[..., Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        # if self.chain_format == ChainFormats.DEFAULT:
        if apply is None:
            return self._extract_options()
        return list(map(apply, self._extract_options()))

    def build_oi_table(self) -> Dict[str, Dict[str, Annotated[List[int], 2]]]:
        table: Dict[str, Dict[str, Annotated[List[int], 2]]] = {}
        callExpDateMap = self.callExpDateMap
        for exp in iter(callExpDateMap):
            table[exp] = {}
            for strike in iter(callExpDateMap[exp]):
                table[exp][strike] = [0, 0]
                for option in iter(callExpDateMap[exp][strike]):
                    oi = int(option["openInterest"])
                    table[exp][strike][1] += oi
        putExpDateMap = self.putExpDateMap
        for exp in iter(putExpDateMap):
            for strike in iter(putExpDateMap[exp]):
                if strike not in table[exp]:
                    table[exp][strike] = [0, 0]
                for option in iter(putExpDateMap[exp][strike]):
                    oi = int(option["openInterest"])
                    table[exp][strike][0] += oi
        return table

    def get_max_pains(self):
        oi_table = self.build_oi_table()
        results_dict = {}
        for expiry in self.expiries:
            results_dict[expiry] = {"max_pain": None, "data": None}
            table_calls = {}
            cumval_calls = 0
            last_strike = None
            cum_oi = 0
            for strike in oi_table[expiry]:
                # OLD:
                # ----
                # if last_strike is not None:
                #     # loop thru each preceding strike to get OI,
                #     # against which we apply the incremental gain
                #     # (the increase in intrinsic value given the
                #     # difference between the current and previous
                #     # strike) to compute the current cumulative
                #     # value of itm options for s in table_calls:
                #     oi_itm = table_calls[s][1]
                #     gain = float(strike) - last_strike
                #     cumval_calls += oi_itm * gain
                # ----------------------------------------------

                # HELPFUL EXHIBIT: CALLS TABLE
                # -----------------------------
                #              cum.      open
                #     strike   value     interest
                #     ------   -----     --------
                #     100.0:   0          0
                #     105.0:   0.0        0
                #     110.0:   0.0        0
                #     115.0:   0.0        15
                #     120.0:   75.0       0    +(120-115)*(15)
                #     125.0:   150.0      0    +(125-120)*(15)
                #     130.0:   225.0      100  +(130-125)*(15)
                #     135.0:   800.0      127  +(135-130)*(15 + 100)
                #     140.0:   2010.0     62   +(140-135)*(15 + 100 + 127)
                #     145.0:   3530.0     20   +(145-140)*(15 + 100 + 127 + 62)
                oi = oi_table[expiry][strike][1]
                if last_strike is not None:
                    cumval_calls += (float(strike) - last_strike) * cum_oi
                cum_oi += oi
                table_calls[float(strike)] = [cumval_calls, oi]
                last_strike = float(strike)

            table_puts = {}
            cumval_puts = 0
            last_strike = None
            cum_oi = 0  # NEW
            for strike in reversed(oi_table[expiry]):
                oi = oi_table[expiry][strike][0]  # NEW
                if last_strike is not None:  # NEW
                    cumval_puts += (last_strike - float(strike)) * cum_oi  # NEW
                cum_oi += oi  # NEW
                table_puts[float(strike)] = [cumval_puts, oi]  # NEW
                last_strike = float(strike)  # NEW

                # OLD
                # ---------
                # if last_strike is not None:
                #     for s in table_puts:
                #         oi_itm = table_puts[s][1]
                #         gain = last_strike - float(strike)
                #         cumval_puts += oi_itm * gain

                # table_puts[float(strike)] = [
                #     cumval_puts,
                #     oi_table[expiry][strike][0],
                # ]
                # last_strike = float(strike)
                # --------------------

            cumval_table = deepcopy(table_puts)
            for strike in cumval_table:
                cumval_table[strike][1] = table_calls[strike][0]
                cumval_table[strike].append(sum(cumval_table[strike]))
            results_dict[expiry]["max_pain"] = self.argmin(cumval_table)
            results_dict[expiry]["data"] = cumval_table
        self.max_pain_results = results_dict
        # return results_dict

    @staticmethod
    def argmin(cumvalues: Dict[float, Annotated[List[int], 3]]):
        """Get max pain given cumvalues dict table

        FInds the strike with the minimum total value for
        a given expiration date.
        """
        # totalval_table = {}
        # for s in cumvalues:
        #     totalval_table[s] = sum(cumvalues[s])
        last_strike = None
        last_value = None
        #             [0]putval [1]callval [2]totalval
        for current_strike, (_, _, current_value) in cumvalues.items():
            if last_value is not None:
                # if the total value at the current strike
                # is greater than the total value at the
                # previous strike, the previous strike
                # is our minimum
                if current_value > last_value:
                    return last_strike
                # if we've hit zero, exit
                if current_value == 0:
                    return current_strike
            last_strike, last_value = current_strike, current_value

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(data=self.all)

    def get_max_pain_fig(self, data, expiry, max_pain) -> go.Figure:
        X = list(sorted(data.keys()))
        X_str = list(map(str, X))
        fig = go.Figure()
        calls_trace = go.Bar(
            name="Calls",
            x=X_str,
            y=[data[x][1] * 100.0 for x in X],
            width=1,
            opacity=0.75,
        )
        puts_trace = go.Bar(
            name="Puts",
            x=X_str,
            y=[data[x][0] * 100.0 for x in X],
            width=1,
            opacity=0.75,
        )
        fig.add_traces([calls_trace, puts_trace])
        title = f"Ticker:{self.underlying['description']}  |  Expiry:{expiry}  |  Max Pain:{max_pain}"
        fig.update_layout(
            title={"text": title, "font": {"family": "Arial"}},
            barmode="stack",
            template="plotly_dark",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(
                orientation="h",
                bgcolor="rgba(0,0,0,0)",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
                font=dict(family="Arial"),
            ),
            font=dict(family="Arial"),
        )

        fig.update_xaxes(type="category")
        return fig

    def plot_max_pain(self, expiry: str) -> None:
        if not hasattr(self, "max_pain_results"):
            self.get_max_pains()

        max_pain = self.max_pain_results[expiry]["max_pain"]
        data = self.max_pain_results[expiry]["data"]
        fig = self.get_max_pain_fig(data, expiry, max_pain)
        fig.show()

    def groupby_dict(self, field: Literal["expiry", "strike"]):
        data = {}

        def lazy(it):
            yield from it

        if field == "expiry":
            callExpDateMap = self.callExpDateMap
            putExpDateMap = self.putExpDateMap
            for expiry in lazy(self.expiries):
                data[expiry] = {}
                call_exp_info = callExpDateMap[expiry]
                put_exp_info = putExpDateMap[expiry]
                for strike in lazy(call_exp_info):
                    if strike not in data[expiry]:
                        data[expiry][strike] = []
                    for option in lazy(call_exp_info[strike]):
                        data[expiry][strike].append(option)
                for strike in lazy(put_exp_info):
                    # options.append(exp_info[strike][0])
                    if strike not in data[expiry]:
                        data[expiry][strike] = []
                    for option in lazy(put_exp_info[strike]):
                        data[expiry][strike].append(option)
        elif field == "strike":
            callExpDateMap = self.callExpDateMap
            putExpDateMap = self.putExpDateMap
            for expiry in lazy(self.expiries):
                # data[expiry] = {}
                call_exp_info = callExpDateMap[expiry]
                put_exp_info = putExpDateMap[expiry]
                for strike in lazy(call_exp_info):
                    if strike not in data:
                        data[strike] = {expiry: []}
                    for option in lazy(call_exp_info[strike]):
                        data[strike][expiry].append(option)
                for strike in lazy(put_exp_info):
                    # options.append(exp_info[strike][0])
                    if strike not in data:
                        data[strike] = {expiry: []}
                    for option in lazy(put_exp_info[strike]):
                        data[strike][expiry].append(option)
        else:
            raise ValueError(
                f"expected `field` to be 'expiry' or 'strike', got '{field}'"
            )
        return data

    def get_strikes(self) -> set[str]:
        strikes: set[str] = set()
        for exp in iter(self.expiries):
            for strike in iter(self.callExpDateMap[exp]):
                strikes.add(strike)
            for strike in iter(self.putExpDateMap[exp]):
                strikes.add(strike)
        return strikes

    @staticmethod
    def _short(option: dict):
        position = deepcopy(option)
        position["debit"] = 0.0
        position["credit"] = option["mark"]
        position["debit_bid"] = 0.0
        position["credit_bid"] = option["bid"]
        position["debit_ask"] = 0.0
        position["credit_ask"] = option["ask"]
        return position

    @staticmethod
    def _long(option: dict):
        position = deepcopy(option)
        position["debit"] = option["mark"]
        position["credit"] = 0.0
        position["debit_bid"] = option["bid"]
        position["credit_bid"] = 0.0
        position["debit_ask"] = option["ask"]
        position["credit_bid"] = 0.0
        return position

    @staticmethod
    def _date_from_expiry(expiry: str) -> dt.date:
        expiry_date_str = expiry.split(":")[0].strip()
        return dt.date.fromisoformat(expiry_date_str)

    @staticmethod
    def _dte_from_expiry(expiry: str) -> int:
        dte_str = expiry.split(":")[1].strip()
        return int(dte_str)

    def _get_monthly_strategy_list_cal(
        self,
    ) -> List[Dict]:
        def get_expiry_date(exp: str) -> dt.date:
            return dt.date.fromisoformat(exp[:10])

        def is_leap(exp: str) -> bool:
            return self._dte_from_expiry(exp) >= 365

        def get_month_str(exp: str) -> str:
            return get_expiry_date(exp).strftime("%b")

        def get_year(exp: str) -> int:
            return get_expiry_date(exp).year

        def get_day(exp: str) -> int:
            return get_expiry_date(exp).day

        strikes: set[str] = set()
        for exp in iter(self.expiries):
            for strike in iter(self.callExpDateMap[exp]):
                strikes.add(strike)
            for strike in iter(self.putExpDateMap[exp]):
                strikes.add(strike)

        monthly_strategy_list = []
        for primaryExpiry in iter(self.expiries):
            for secondaryExpiry in iter(self.expiries):
                if primaryExpiry != secondaryExpiry:
                    month = get_month_str(primaryExpiry)
                    year = get_year(primaryExpiry)
                    day = get_day(primaryExpiry)
                    daysToExp = self._dte_from_expiry(primaryExpiry)
                    secondaryMonth = get_month_str(secondaryExpiry)
                    secondaryYear = get_year(secondaryExpiry)
                    secondaryDay = get_day(secondaryExpiry)
                    secondaryDaysToExp = self._dte_from_expiry(secondaryExpiry)
                    leap = is_leap(primaryExpiry)
                    secondaryLeap = is_leap(secondaryExpiry)
                    for type, secondaryType in (
                        ("PUT", "PUT"),
                        # ("PUT", "CALL"),
                        # ("CALL", "PUT"),
                        ("CALL", "CALL"),
                    ):
                        monthly_strategy = dict(
                            month=month,
                            year=year,
                            day=day,
                            daysToExp=daysToExp,
                            secondaryMonth=secondaryMonth,
                            secondaryYear=secondaryYear,
                            secondaryDay=secondaryDay,
                            secondaryDaysToExp=secondaryDaysToExp,
                            type=type,
                            secondaryType=secondaryType,
                            leap=leap,
                            secondaryLeap=secondaryLeap,
                            optionStrategyList=[],
                        )
                        master_data = []
                        for strike in strikes:
                            if spread := self._get_calendar_spread(
                                strike,
                                primaryExpiry,
                                secondaryExpiry,
                                type,
                                secondaryType,
                            ):
                                master_data.append(spread)
                        monthly_strategy["optionStrategyList"] = master_data
                        monthly_strategy_list.append(monthly_strategy)
        return monthly_strategy_list

    def _get_calendar_leg(self, option: OptionDictType) -> OptionDictType:
        return {
            "symbol": option["symbol"],
            "putCallInd": option["putCall"][0],
            "description": option["description"],
            "bid": option["bid"],
            "ask": option["ask"],
            "range": "ITM" if option["inTheMoney"] else "OTM",
            "strikePrice": option["strikePrice"],
            "settlementType": option["settlementType"],
            "expirationType": option["expirationType"],
            "lastPrice": option["last"],
            "mark": option["mark"],
            "bidSize": option["bidSize"],
            "askSize": option["askSize"],
            "lastSize": option["lastSize"],
            "highPrice": option["highPrice"],
            "lowPrice": option["lowPrice"],
            "high52Week": option["high52Week"],
            "low52Week": option["low52Week"],
            "openPrice": option["openPrice"],
            "closePrice": option["closePrice"],
            "openInterest": option["openInterest"],
            "netChange": option["netChange"],
            "volatility": option["volatility"],
            "delta": option["delta"],
            "gamma": option["gamma"],
            "theta": option["theta"],
            "vega": option["vega"],
            "rho": option["rho"],
            "tradeTimeInLong": option["tradeTimeInLong"],
            "quoteTimeInLong": option["quoteTimeInLong"],
            "percentChange": option["percentChange"],
            "intrinsicValue": option["intrinsicValue"],
            "extrinsicValue": option["extrinsicValue"],
            "volume": option["totalVolume"],
        }

    def _get_calendar_spread(
        self,
        strike: Union[float, str],
        primaryExpiry: str,
        secondaryExpiry: str,
        type: str,
        # type: Literal["CALL", "PUT"],
        secondaryType: str,
        # secondaryType: Literal["CALL", "PUT"],
    ):
        strike = float(strike)
        strategy = {}
        long_expiry_date = self._date_from_expiry(primaryExpiry)
        short_expiry_date = self._date_from_expiry(secondaryExpiry)

        def get_expiry(o: OptionDictType) -> dt.date:
            return dt.date.fromisoformat(o["expirationDate"][:10])

        def get_strike(o: OptionDictType) -> float:
            return o["strikePrice"]

        def get_putCall(o: OptionDictType) -> str:
            return o["putCall"]

        def get_month_str(o: OptionDictType) -> str:
            return get_expiry(o).strftime("%b")

        def long_key(o: OptionDictType) -> bool:
            return (
                (get_expiry(o) == long_expiry_date)
                and (get_strike(o) == strike)
                and (get_putCall(o) == type)
            )

        def short_key(o: OptionDictType) -> bool:
            return (
                (get_expiry(o) == short_expiry_date)
                and (get_strike(o) == strike)
                and (get_putCall(o) == secondaryType)
            )

        try:
            long_opt = list(filter(long_key, self.all))[0]
            short_opt = list(filter(short_key, self.all))[0]
        except Exception:
            pass
        else:
            strategy["primaryLeg"] = self._get_calendar_leg(long_opt)
            strategy["secondaryLeg"] = self._get_calendar_leg(short_opt)
            strategy["strategyStrike"] = (
                f"{get_month_str(long_opt)}/{get_month_str(short_opt)}"
            )
            strategy["strategyBid"] = short_opt["bid"] - long_opt["ask"]
            strategy["strategyAsk"] = short_opt["ask"] - long_opt["bid"]
            strategy["strategyMark"] = short_opt["mark"] - long_opt["mark"]
            strategy["strategyDelta"] = long_opt["delta"] + short_opt["delta"]
            strategy["strategyGamma"] = long_opt["gamma"] + short_opt["gamma"]
            strategy["strategyTheta"] = long_opt["theta"] + short_opt["theta"]
            strategy["strategyVega"] = long_opt["vega"] + short_opt["vega"]
            strategy["strategyRho"] = long_opt["rho"] + short_opt["rho"]
            return strategy

    def calendar_spreads(self):
        data = OrderedDict()
        data["symbol"] = self.chain["symbol"]
        data["status"] = self.chain["status"]
        data["underlying"] = self.underlying
        data["strategy"] = "CALENDAR"
        data["interval"] = self.chain["interval"]
        data["isDelayed"] = self.chain["isDelayed"]
        data["isIndex"] = self.chain["isIndex"]
        data["interestRate"] = self.chain["interestRate"]
        data["underlyingPrice"] = self.chain["underlyingPrice"]
        data["volatility"] = self.chain["volatility"]
        data["daysToExpiration"] = self.chain["daysToExpiration"]
        data["numberOfContracts"] = self.chain["numberOfContracts"]
        data["assetMainType"] = self.chain["assetMainType"]
        data["assetSubType"] = self.chain["assetSubType"]
        data["isChainTruncated"] = self.chain["isChainTruncated"]
        data["intervals"] = self.chain.get("intervals", [])
        data["monthlyStrategyList"] = self._get_monthly_strategy_list_cal()
        data["callExpDateMap"] = {}
        data["putExpDateMap"] = {}
        data["fetchTime"] = self.fetchTime
        return data

    def _add_underlying_price(self, option_dict: OptionDictType) -> OptionDictType:
        if "underlyingPrice" not in option_dict:
            option_dict["underlyingPrice"] = self.underlying_price
        return option_dict

    def prepare_dict(self, option_dict: OptionDictType) -> OptionDictType:
        # option_dict["underlyingPrice"] = self.underlying_price
        option_dict = self._add_underlying_price(option_dict)
        if (option_dict["intrinsicValue"] < option_dict["mark"]) and (
            option_dict["daysToExpiration"] >= 0
        ):
            option_dict["extra"] = {
                "t": self.market_calendar(
                    option_dict["daysToExpiration"], self.fetchTimeDt
                ),
                "q": self.dividend_yield,
            }
            assert yts is not None  # mypy
            assert ive is not None  # mypy
            # option_dict["extra"]["r"] = yts.get_rate(option_dict["daysToExpiration"])
            option_dict["extra"]["r"] = continuously_compounding_rate(
                yts.get_rate(option_dict["daysToExpiration"])
            )  # NEW
            option_dict["iv_mark"] = ive.find_implied_volatility(
                option_dict["mark"],
                option_dict["underlyingPrice"],
                option_dict["strikePrice"],
                option_dict["daysToExpiration"],
                option_dict["extra"]["q"],
                float(option_dict["extra"]["r"]),
                option_dict["putCall"],
                option_dict["exerciseType"],
            )
            option_dict["extra"] = json.dumps(option_dict["extra"])
        else:
            option_dict["iv_mark"] = None
        return option_dict

    def delta_neutral(self):
        pass

    # def add_global_fields(self, _dict: Dict[str, Any]):
    #     """
    #     Append each option dictionary with data not included
    #     in the json for each option and return an encoded
    #     json string.

    #     Args:
    #         `_dict` (`dict[str, Any`):
    #             Dictionary containing attributes of a single option.

    #     Returns:
    #         `bytes`:
    #             Encoded json string.
    #     """
    #     _dict["underlyingPrice"] = self.underlying_price
    #     _dict["q"] = self.dividend_yield
    #     _dict["t"] = self.market_calendar(_dict["daysToExpiration"])
    #     _dict["r"] = self.iv_engine.term_structure.get_rate(_dict["daysToExpiration"])
    #     try:
    #         _dict["iv_mark"] = self.iv_engine(
    #             market_price=_dict["mark"],
    #             S=_dict["underlyingPrice"],
    #             K=_dict["strikePrice"],
    #             dte=_dict["daysToExpiration"],
    #             q=_dict["q"],
    #             putCall=_dict["putCall"],
    #             exercise_type=_dict["exerciseType"],
    #             r=_dict["r"],
    #         )

    #     except Exception as e:
    #         print(f"ERROR: Failed to calculate iv_mark\n{e}")
    #         _dict["iv_mark"] = None
    #     finally:
    #         return self.encoder.encode(_dict)
    #         # _dict["r"] = 0.0
    #     # else:
    #     #     _dict["iv_mark"] = self.iv_engine(
    #     #         market_price=_dict["mark"],
    #     #         S=_dict["underlyingPrice"],
    #     #         K=_dict["StrikePrice"],
    #     #         dte=_dict["daysToExpiration"],
    #     #         q=_dict["q"],
    #     #         putCall=_dict["putCall"],
    #     #         r=_dict["r"],
    #     #     )
    #     # return self.encoder.encode(_dict)

    # def get_primary_keys(self) -> list[tuple[str, datetime.datetime]]:
    #     """
    #     Returns a list of tuples containing
    #     symbol and trade_time for each option
    #     in self.all
    #     """
    #     return [option._primary_key for option in self.all]

    # def filter(self, keys: list[tuple[str, datetime.datetime]]):
    #     """
    #     Remove all options from self.all whose _primary_key in keys
    #     """
    #     self.filtered = True

    #     self.all = [
    #         option for option in iter(self.all)
    #         if option._primary_key not in keys
    #     ]

    # def get_temphandler(self, oi: bool = False):
    #     if oi:
    #         extra_commas = 0
    #         if self.test_insert:
    #             failover_persistent_path = "oi_test_failover"
    #             sql_table = ".".join(
    #                 [self.SQL_DB, self.SQL_SCHEMA, self.SQL_TEST_TABLE_OI]
    #             )
    #         else:
    #             failover_persistent_path = "oi_failover"
    #             sql_table = self.SQL_TABLE_OI
    #     else:
    #         extra_commas = 1
    #         if self.test_insert:
    #             failover_persistent_path = "option_rest_test_failover"
    #             sql_table = ".".join(
    #                 [self.SQL_DB, self.SQL_SCHEMA, self.SQL_TEST_TABLE]
    #             )
    #         else:
    #             failover_persistent_path = "option_rest_failover"
    #             sql_table = self.SQL_TABLE_NO_PK

    #     writer = RemoteTempHandler(
    #         host=os.environ["SQL_PRIMARY_SERVER"],
    #         username=os.environ["SQL_PRIMARY_SERVER_USERNAME"],
    #         password=os.environ["SQL_PRIMARY_SERVER_PASSWORD"],
    #     )

    #     return writer, extra_commas, sql_table, failover_persistent_path

    # def insert_all(self, oi: bool = False, delete: bool = True) -> Optional[StrPath]:
    #     # sourcery skip: remove-unnecessary-else, swap-if-else-branches
    #     """Insert all options to sql.

    #     :param oi (bool, optional):
    #         Insert open interest values to sql.
    #         Defaults to False.
    #     :param delete (bool, optional):
    #         Delete the local tempfile.
    #         Defaults to True.
    #     """

    #     csvs = self.get_csvs(oi=oi)

    #     (writer, extra_commas, sql_table, failover_persistent_path) = (
    #         self.get_temphandler(oi=oi)
    #     )

    #     tmpfilepath = writer.write(
    #         lines=csvs,
    #         sql_table=sql_table,
    #         extra_commas=extra_commas,
    #         failover_persistent_path=failover_persistent_path,
    #     )

    #     if tmpfilepath is not None:
    #         if delete and os.path.exists(tmpfilepath):
    #             os.remove(tmpfilepath)
    #         return None
    #     else:
    #         return tmpfilepath

    def get_csvs(self, oi: bool = False) -> List[str]:
        """
        returns csv representation of all options
        """
        encoded_options = list(map(self.encoder.encode, self.all))
        options = list(map(self.decoder.decode, encoded_options))
        if oi:

            raw_csvs = list(map(lambda x: x._to_csv(oi=True), options))

            def modify_csv(csv: str):

                def listify(csv: str, delim: str = ",") -> List[str]:
                    """Return csv as a list of elements."""
                    return csv.split(delim)

                def underlying_from_symbol(symbol: str) -> str:
                    """Extract underlying ticker symbol"""
                    return symbol.split("_")[0]

                def ts_to_datestr(ts: Union[str, int]) -> str:
                    """Convert expiry timestamp to an ISO-formatted string."""
                    if isinstance(ts, str):
                        ts = int(ts)
                    return dt.datetime.fromtimestamp(ts / 1000).date().isoformat()

                list_elements = listify(csv)

                # extract symbol and expiry timestamp
                symbol, _, _, expiry_ts, _, _ = list_elements

                # update list of elements
                list_elements[3] = ts_to_datestr(expiry_ts)

                underlying = underlying_from_symbol(symbol)
                # date = (
                #     self.date.isoformat()
                #     if isinstance(self.date, datetime.date)
                #     else self.date
                # )
                # date = dt.datetime.fromisoformat(self.fetchTime).date().isoformat()
                date = self.fetchTime[:10]
                list_elements.extend([date, underlying])

                return ",".join(list_elements)

            return list(map(modify_csv, raw_csvs))

        else:

            def scientific_notation_to_float(csv: str) -> str:
                """
                Ensures float values of csv are formatted as decimals
                and not in scientific notation, which Python sometimes
                chooses to do.

                Args:
                    csv (str): option attrs as a csv string

                Returns:
                    str: option attrs as a csv string, with float values
                        properly formatted for insertion into SQL Server
                """
                # convert csv string to list
                _list = csv.split(",")
                idxs_of_floats = [
                    4,  # delta
                    6,  # gamma
                    9,  # iv_mark
                    17,  # q
                    19,  # r
                    20,  # rho
                    23,  # theta
                    26,  # vega
                ]

                def format_float_str(val: str) -> str:
                    # val = _list[idx]
                    return format(float(val), "f") if val else val

                for idx in idxs_of_floats:
                    _list[idx] = format_float_str(val=_list[idx])

                # convert list to csv string
                csv = ",".join(_list)

                return csv

            csvs = list(map(lambda x: x._to_csv(), options))
            return list(map(scientific_notation_to_float, csvs))

    @staticmethod
    def create_options_symbol(
        optionRoot: str, expirationDate: str, putCall: str, strikePrice: float
    ) -> str:
        """Construct the option symbol given the `optionRoot`, `expirationDate`,
        `putCall`, and `strikePrice` fields, as provided in the option dictionary

        :param optionRoot: the underlying ticker
        :type optionRoot: str
        :param expirationDate: the expiration date
        :type expirationDate: str
        :param putCall: "PUT" or "CALL"
        :type putCall: str
        :param strikePrice: the strike price
        :type strikePrice: float
        :return: the option symbol
        :rtype: str
        """
        expiry_date = dt.date.fromisoformat(expirationDate[:10]).strftime("%y%m%d")
        putCall = putCall[0].upper()
        strike = str(int(strikePrice * 1000)).zfill(8)
        return f"{optionRoot}  {expiry_date}{putCall}{strike}"

    def get_option(self, option_symbol: str) -> Optional[OptionDictType]:
        queried = list(filter(lambda x: x["symbol"] == option_symbol, self.all))
        if queried:
            return queried[0]

    # @classmethod
    # def fetch_chain(cls, underlying: str, acct_no: int = 1) -> dict:
    #     """Fetches and returns option chain data for the underlying
    #     security passed.

    #     Args:
    #         `underlying` (`str`):
    #             Underlying ticker for which to fetch option chain.
    #         `acct_no` (`int`, optional, default=1):
    #             TD Ameritrade account to use when authenticating
    #             get request for option chain data.

    #     Raises:
    #         HTTPError:
    #             If status code of response does not equal 200.

    #     Returns:
    #         dict:
    #             JSON of response to option chain get request to TD Ameritrade.
    #     """
    #     from tda.auth import easy_client
    #     from tda.client import Client as TDClient

    #     from daily.tda.accounts import Account
    #     from daily.tda.accounts import get_credentials

    #     credentials: Account = get_credentials(acct_no=acct_no)

    #     resp: requests.Response = easy_client(
    #         api_key=credentials.api_key,
    #         redirect_uri=credentials.redirect_uri,
    #         token_path=credentials.token_path,
    #     ).get_option_chain(
    #         symbol=underlying,
    #         contract_type=TDClient.Options.ContractType.ALL,
    #         strategy=TDClient.Options.Strategy.ANALYTICAL,
    #         strike_range=TDClient.Options.StrikeRange.ALL,
    #         option_type=TDClient.Options.Type.ALL,
    #         include_quotes=True,
    #     )

    #     resp.raise_for_status()
    #     return resp.json()

    def __len__(self):
        return len(self.all)

    def __repr__(self):
        return (
            f"{__name__}.{self.__class__.__name__}"
            f"<{self.underlying['symbol']} @ "
            f"{self.fetchTime}>"
        )

    # @classmethod
    # def from_underlying(cls, underlying: str, acct_no: int = 1) -> OptionChain:
    #     """Instantiate option chain with an underlying symbol.
    #     The option chain data will be fetched for you.
    #     """
    #     data = cls.fetch_chain(underlying=underlying, acct_no=acct_no)
    #     return cls(data=data)
