# import datetime as dt
from typing import cast
from typing import Literal
from typing import Optional

# from typing import Protocol

from scipy.optimize import brentq

from daily.options import price_engine as price_engine

# from daily import term_structure as yts


# class PricingEngineProto(Protocol):
#     def get_price(
#         self,
#         sigma: float,
#         S: float,
#         K: float,
#         dte: int,
#         r: float,
#         q: float,
#         putCall: Literal["CALL", "PUT"],
#     ) -> float: ...


# class YieldTermStructureProto(Protocol):
#     def get_rate(self, t: float) -> float: ...


# class MarketCalendarProto(Protocol):
#     def __call__(self, dte: int) -> float: ...


# from daily.options.price_engine import BinomialTreePricingEngine
# from daily.options.price_engine import BlackScholesPricingEngine
# from daily.term_structure import CubicSplineYieldTermStructure
# from daily.term_structure import FlatForwardYieldTermStructure
# from daily.term_structure import FlatYieldTermStructure
# from daily.term_structure import NelsonSiegelYieldTermStructure
# from daily.term_structure import PiecewiseLinearYieldTermStructure
# from daily.term_structure import SvenssonYieldTermStructure


class ImpliedVolatilityEngine:

    # def __init__(
    #     self,
    #     # pricing_engine: PricingEngineProto,
    #     term_structure: YieldTermStructureProto,
    #     # market_calendar: MarketCalendarProto,
    # ) -> None:
    #     # self.pricing_engine = pricing_engine
    #     self.term_structure = term_structure
    #     # self.market_calendar = market_calendar

    # def get_r(self, dte: int):
    #     t = self.market_calendar(dte)
    #     return self.term_structure.get_rate(t)

    def get_price_engine(self, exercise_type: str):
        if exercise_type == "A":
            return self.PriceEngines.BinomialTree(N=100)
        elif exercise_type == "E":
            return self.PriceEngines.BlackScholes()
        else:
            raise ValueError(f"Unrecognized exercise type '{exercise_type}'")

    def find_implied_volatility(
        self,
        market_price: float,
        S: float,
        K: float,
        # dte: int,
        t: float,
        q: float,
        r: float,
        # putCall: Literal["CALL", "PUT"],
        putCall: str,
        # exercise_type: Literal["A", "E"],
        exercise_type: str,
    ):
        # # if putCall == "CALL":
        # #     intrinsic = max(S - K, 0)
        # # else:
        # #     intrinsic = max(K - S, 0)
        # if intrinsic == 0:
        #     return 0.0
        if exercise_type not in ("A", "E"):
            exercise_type = "A"
        price_engine = self.get_price_engine(exercise_type)

        def objective_function(sigma):
            return price_engine.get_price(sigma, S, K, t, r, q, putCall) - market_price

        try:
            return cast(float, brentq(objective_function, 1e-2, 10.0, xtol=1e-6))
        except ValueError:
            return None

    find = find_implied_volatility

    # def get_implied_volatility(
    #     self,
    #     market_price: float,
    #     S: float,
    #     K: float,
    #     dte: int,
    #     q: float,
    #     putCall: Literal["CALL", "PUT"],
    #     exercise_type: Literal["A", "E"],
    #     r: Optional[float] = None,
    # ) -> Optional[float]:
    #     # t = self.market_calendar(dte)
    #     # r = self.term_structure.get_rate(t)

    #     if putCall == "CALL":
    #         intrinsic = max(S - K, 0)
    #     else:
    #         intrinsic = max(K - S, 0)
    #     if intrinsic == 0:
    #         return 0.0

    #     r = r or self.get_r(dte)
    #     price_engine = self.get_price_engine(exercise_type)

    #     def objective_function(sigma):
    #         return (
    #             price_engine.get_price(sigma, S, K, dte, r, q, putCall) - market_price
    #         )

    #     try:
    #         return cast(float, brentq(objective_function, 1e-2, 10.0, xtol=1e-6))
    #     except ValueError:
    #         return None

    # def _compute_t(self, dte: int):
    #     mkt_day_secs = 6.5 * 60 * 60
    #     remaining = (dte - 1) * mkt_day_secs

    def __call__(
        self,
        market_price: float,
        S: float,
        K: float,
        # dte: int,
        t: float,
        q: float,
        r: float,
        putCall: Literal["CALL", "PUT"],
        exercise_type: Literal["A", "E"],
    ) -> Optional[float]:
        return self.find_implied_volatility(
            market_price, S, K, t, q, r, putCall, exercise_type
        )

    class PriceEngines:
        BinomialTree = price_engine.BinomialTreePricingEngine
        BlackScholes = price_engine.BlackScholesPricingEngine

    # class YieldTermStructures:
    #     CubicSpline = yts.CubicSplineYieldTermStructure
    #     FlatForward = yts.FlatForwardYieldTermStructure
    #     Flat = yts.FlatYieldTermStructure
    #     NelsonSiegel = yts.NelsonSiegelYieldTermStructure
    #     PiecewiseLinear = yts.PiecewiseLinearYieldTermStructure
    #     Svensson = yts.SvenssonYieldTermStructure
