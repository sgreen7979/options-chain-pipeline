#!/usr/bin/env python3
from typing import Literal
from typing import Union

from scipy.optimize import brentq

import blackscholes
import binomialtree


class PricingEngine:
    def get_price(
        self,
        sigma: float,
        S: float,
        K: float,
        t: float,
        r: float,
        q: float,
        putCall: Literal["CALL", "PUT"],
    ) -> float:
        raise NotImplementedError("must implement in a subclass")


class BinomialTreePricingEngine(PricingEngine):
    def __init__(self, N: int = 1000) -> None:
        super().__init__()
        self.N = N

    def get_price(
        self,
        sigma: float,
        S: float,
        K: float,
        t: float,
        r: float,
        q: float,
        # putCall: Literal["CALL", "PUT"],
        putCall: str,
    ) -> float:
        return binomialtree.price(S, K, t, r, sigma, q, putCall, self.N)  # type: ignore


class BlackScholesPricingEngine(PricingEngine):

    def get_price(
        self,
        sigma: float,
        S: float,
        K: float,
        t: float,
        r: float,
        q: float,
        # putCall: Literal["CALL", "PUT"],
        putCall: str,
    ) -> float:
        return blackscholes.price(S, K, t, r, sigma, q, putCall)  # type: ignore


def find_implied_volatility(
    pricing_engine: Union[BinomialTreePricingEngine, BlackScholesPricingEngine],
    market_price: float,
    S: float,
    K: float,
    t: float,
    r: float,
    q: float,
    putCall: Literal["CALL", "PUT"],
):
    def objective_function(sigma):
        return pricing_engine.get_price(sigma, S, K, t, r, q, putCall) - market_price

    try:
        return brentq(objective_function, 1e-2, 3.0, xtol=1e-6)
    except ValueError:
        return None


def main():
    # Example usage
    S = 100  # Underlying stock price
    K = 100  # Strike price
    # T = 1.0  # Time to expiration in years
    # dte = 17  # Days to expiration
    t = 0.5  # Time to expiration in years
    r = 0.05  # Risk-free interest rate
    q = 0.01  # Dividend yield
    market_price = 10.0  # Market price of the option
    # N = 500  # Number of steps in the binomial model
    option_type = 'CALL'  # 'call' or 'put'

    pricing_engine = BinomialTreePricingEngine()
    implied_vol = find_implied_volatility(
        pricing_engine, market_price, S, K, t, r, q, option_type
    )
    if implied_vol is not None:
        print(f"The implied volatility is: {implied_vol * 100:.2f}%")
    else:
        print("The implied volatility is: None")


if __name__ == "__main__":
    main()
