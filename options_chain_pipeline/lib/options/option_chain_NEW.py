#!/usr/bin/env python3

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
import datetime as dt
import json
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

from daily.options.decoder import Decoder
from daily.options.encoder import Encoder
from daily.options.iv_engine import ImpliedVolatilityEngine as IVEngine
from daily.options.mcal.calendars import MarketCalendars
from daily.options.mcal.parallel import MarketCalendar
from daily.rates import continuously_compounding_rate
from daily.term_structure import SvenssonYieldTermStructure
from daily.utils.logging import get_logger

logger = get_logger(__name__, ch=True, propagate=False)


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
    ive = IVEngine(yts)


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
ApplyFunctionType = Callable[..., OptionDictType]


class underlyingInfo(TypedDict):
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


class OptionChain:
    """Parses and manages option chain data from TD Ameritrade.

    Args:
        `data` (`dict`):
            JSON of response to get request from TD Ameritrade.

        `dividend_amt` (`float`, optional):
            Last dividend amount.
            Defaults to None, in which case it will be fetched.

        `decoder` (`Decoder`, optional):
            Class that handles decoding individual options.
            Defaults to `Decoder`.

        `encoder` (`Encoder`, optional):
            Class that handles encoding individual options.
            Defaults to `Encoder`.
    """

    def __init__(
        self,
        data: dict,
        dividend_amt: Optional[float] = None,
        apply: Optional[ApplyFunctionType] = None,
        decoder: Optional[Decoder] = None,
        encoder: Optional[Encoder] = None,
        **kwargs,
    ) -> None:
        self.chain = data
        self._dividend_amt = dividend_amt or 0.0
        if apply is not None:
            self.market_calendar = get_cal(self.underlying["exchangeName"])
            get_ive()

        self.filtered: bool = False
        self._expiries: set = set()
        self.decoder = decoder
        self.encoder = encoder
        self.fetchTime: dt.datetime = self.chain.get("fetch_time", dt.datetime.now())

        for key, val in kwargs.items():
            setattr(self, key, val)

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

    @property
    def underlying(self) -> underlyingInfo:
        return self.chain["underlying"]

    @property
    def expiries(self) -> List[str]:
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
        return continuously_compounding_rate(self._dividend_amt / self.underlying_price)

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
        if apply is None:
            return self._extract_options()
        return list(map(apply, self._extract_options()))

    def build_oi_table(self):
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
                #     # loop thru each preceding strike
                #     # to get OI, against which we
                #     # apply the incremental gain
                #     # (the increase in intrinsic value
                #     # given the difference between the
                #     # current and previous strike)
                #     # to compute the current cumulative
                #     # value of itm options
                #     for s in table_calls:
                #         oi_itm = table_calls[s][1]
                #         gain = float(strike) - last_strike
                #         cumval_calls += oi_itm * gain
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
            for strike in reversed(oi_table[expiry]):
                if last_strike is not None:
                    for s in table_puts:
                        oi_itm = table_puts[s][1]
                        gain = last_strike - float(strike)
                        cumval_puts += oi_itm * gain

                table_puts[float(strike)] = [
                    cumval_puts,
                    oi_table[expiry][strike][0],
                ]
                last_strike = float(strike)

            cumval_table = deepcopy(table_puts)
            for strike in cumval_table:
                cumval_table[strike][1] = table_calls[strike][0]
                cumval_table[strike].append(sum(cumval_table[strike]))
            results_dict[expiry]["max_pain"] = self.argmin(cumval_table)
            results_dict[expiry]["data"] = cumval_table
        self.max_pain_results = results_dict

    @staticmethod
    def argmin(cumvalues: Dict[float, Annotated[List[int], 3]]):
        """Get max pain given cumvalues dict table

        FInds the strike with the minimum total value for
        a given expiration date.
        """
        last_strike = None
        last_value = None
        #             [0]putval [1]callval [2]totalval
        for current_strike, (_, _, current_value) in cumvalues.items():
            if last_value is not None:
                # if the total value at the current strike is greater
                # than the total value at the previous strike, the
                # previous strike is our minimum
                if current_value > last_value:
                    return last_strike
                # if we've hit zero, the current strike is our minimum
                if current_value == 0:
                    return current_strike
            last_strike, last_value = current_strike, current_value

    def to_df(self):
        return pd.DataFrame(data=self.all)

    def plot_max_pain(self, expiry: str):
        if not hasattr(self, "max_pain_results"):
            self.get_max_pains()

        max_pain = self.max_pain_results[expiry]["max_pain"]
        data = self.max_pain_results[expiry]["data"]
        title = f"""
Ticker: {self.underlying['description']} |
Expiry: {expiry} |
Max Pain: {max_pain}
"""
        X = list(sorted(data.keys()))
        X_str = list(map(str, X))
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                name="Calls",
                x=X_str,
                y=[data[x][1] * 100.0 for x in X],
                width=1,
                opacity=0.75,
            )
        )
        fig.add_trace(
            go.Bar(
                name="Puts",
                x=X_str,
                y=[data[x][0] * 100.0 for x in X],
                width=1,
                opacity=0.75,
            )
        )

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
        fig.show()

    def groupby_dict(self, field: Literal["expiry", "strike"]):
        data = {}
        callExpDateMap = self.callExpDateMap
        putExpDateMap = self.putExpDateMap

        if field == "expiry":
            for expiry in iter(self.expiries):
                data[expiry] = {}
                for strike in iter(callExpDateMap[expiry]):
                    if strike not in data[expiry]:
                        data[expiry][strike] = []
                    for option in iter(callExpDateMap[expiry][strike]):
                        data[expiry][strike].append(option)
                for strike in iter(putExpDateMap[expiry]):
                    if strike not in data[expiry]:
                        data[expiry][strike] = []
                    for option in iter(putExpDateMap[expiry][strike]):
                        data[expiry][strike].append(option)

        elif field == "strike":
            for expiry in iter(self.expiries):
                for strike in iter(callExpDateMap[expiry]):
                    if strike not in data:
                        data[strike] = {expiry: []}
                    for option in iter(callExpDateMap[expiry][strike]):
                        data[strike][expiry].append(option)
                for strike in iter(putExpDateMap[expiry]):
                    if strike not in data:
                        data[strike] = {expiry: []}
                    for option in iter(putExpDateMap[expiry][strike]):
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
                        ("PUT", "CALL"),
                        ("CALL", "PUT"),
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

    def _get_calendar_spread(
        self,
        strike: Union[float, str],
        primaryExpiry: str,
        secondaryExpiry: str,
        type: Literal["CALL", "PUT"],
        secondaryType: Literal["CALL", "PUT"],
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
            strategy["primaryLeg"] = long_opt
            strategy["secondaryLeg"] = short_opt
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
        data["callExpDateMap"] = {}  # intentionally left blank to limit size
        data["putExpDateMap"] = {}  # intentionally left blank to limit size
        data["fetchTime"] = self.fetchTime.isoformat()
        return data

    def _add_underlying_price(self, option_dict: OptionDictType) -> OptionDictType:
        if "underlyingPrice" not in option_dict:
            option_dict["underlyingPrice"] = self.underlying_price
        return option_dict

    def prepare_dict(self, option_dict: OptionDictType) -> OptionDictType:
        option_dict = self._add_underlying_price(option_dict)
        if (option_dict["intrinsicValue"] < option_dict["mark"]) and (
            option_dict["daysToExpiration"] >= 0
        ):
            option_dict["extra"] = {
                "t": self.market_calendar(option_dict["daysToExpiration"]),
                "q": self.dividend_yield,
            }
            assert yts is not None  # mypy
            assert ive is not None  # mypy
            option_dict["extra"]["r"] = yts.get_rate(option_dict["daysToExpiration"])
            option_dict["iv_mark"] = ive.find_implied_volatility(
                option_dict["mark"],
                option_dict["underlyingPrice"],
                option_dict["strikePrice"],
                option_dict["daysToExpiration"],
                option_dict["extra"]["q"],
                option_dict["extra"]["r"],
                option_dict["putCall"],
                option_dict["exerciseType"],
            )
            option_dict["extra"] = json.dumps(option_dict["extra"])
        else:
            option_dict["iv_mark"] = None
        return option_dict

    def delta_neutral(self):
        pass

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
                date = self.fetchTime.date().isoformat()
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

    def __len__(self):
        return len(self.all)

    def __repr__(self):
        return (
            f"{__name__}.{self.__class__.__name__}"
            f"<{self.underlying['symbol']} @ "
            f"{self.fetchTime.isoformat()}>"
        )
