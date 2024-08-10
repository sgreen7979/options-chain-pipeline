#!/usr/bin/env python3
from typing import cast
from typing import Literal
from typing import Optional

from scipy.optimize import brentq

from .. import price_engine as price_engine


class ImpliedVolatilityEngine:

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
        t: float,
        q: float,
        r: float,
        putCall: str,
        exercise_type: str,
    ):
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

    def __call__(
        self,
        market_price: float,
        S: float,
        K: float,
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
