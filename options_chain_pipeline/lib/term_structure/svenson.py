#!/usr/bin/env python3
import datetime as dt
from typing import cast
from typing import Dict
from typing import Optional

import numpy as np
from scipy.optimize import minimize

from .base import YieldTermStructure
from .types import MarketDataType
from .types import SvensonParamsType

# from daily.options.mcal.parallel import MarketCalendar


class SvenssonYieldTermStructure(YieldTermStructure):

    default_initial_guess: SvensonParamsType = [0.03, -0.02, 0.02, 0.01, 1.0, 2.0]

    def __init__(
        self,
        reference_date: dt.date,
        data_points: MarketDataType,
        *,
        initial_guess: Optional[SvensonParamsType] = None,
    ) -> None:
        """
        Initialize Svensson Yield Term Structure and calibrate parameters.

        :param reference_date: The reference date for the yield curve.
        :type reference_date: dt.date
        :param data_points: List of tuples with dates and corresponding yields.
        :type data_points: List[Tuple[dt.date, float]]
        """
        super().__init__(reference_date, data_points)
        self.initial_guess: SvensonParamsType = (
            initial_guess or self.default_initial_guess
        )
        # self.market_calendar = MarketCalendar()
        self.calibrate()

    def get_rate(self, t: float) -> float:
        """
        Calculate the Svensson interest rate for a given time t.

        :param t: Time to maturity in years.
        :type t: float
        :return: The interest rate.
        :rtype: float
        """
        term1 = self.beta0
        term2 = self.beta1 * (1 - np.exp(-t / self.tau1)) / (t / self.tau1)
        term3 = self.beta2 * (
            (1 - np.exp(-t / self.tau1)) / (t / self.tau1) - np.exp(-t / self.tau1)
        )
        term4 = self.beta3 * (
            (1 - np.exp(-t / self.tau2)) / (t / self.tau2) - np.exp(-t / self.tau2)
        )
        return term1 + term2 + term3 + term4

    # def get_rate_dte(self, dte: int) -> float:
    #     """
    #     Calculate the Svensson interest rate for a given time t.

    #     :param dte: Days to expiration.
    #     :type dte: int
    #     :return: The interest rate.
    #     :rtype: float
    #     """
    #     t = self.market_calendar(dte)
    #     term1 = self.beta0
    #     term2 = self.beta1 * (1 - np.exp(-t / self.tau1)) / (t / self.tau1)
    #     term3 = self.beta2 * (
    #         (1 - np.exp(-t / self.tau1)) / (t / self.tau1) - np.exp(-t / self.tau1)
    #     )
    #     term4 = self.beta3 * (
    #         (1 - np.exp(-t / self.tau2)) / (t / self.tau2) - np.exp(-t / self.tau2)
    #     )
    #     return term1 + term2 + term3 + term4

    def objective_sv(self, params: SvensonParamsType) -> float:
        """
        Objective function for minimizing the difference between observed and
        model yields.

        :param params: List of parameters [beta0, beta1, beta2, beta3, tau1, tau2].
        :type params: List[float]
        :return: Sum of squared differences between observed and model yields.
        :rtype: float
        """
        self.beta0, self.beta1, self.beta2, self.beta3, self.tau1, self.tau2 = params
        predicted_yields = list(map(self.get_rate, self.times))
        return cast(
            float, np.sum((np.array(predicted_yields) - np.array(self.yields)) ** 2)
        )

    def calibrate(self) -> None:
        """
        Calibrate the Svensson model parameters.
        """
        self.beta0, self.beta1, self.beta2, self.beta3, self.tau1, self.tau2 = tuple(
            minimize(self.objective_sv, self.initial_guess, method='Nelder-Mead').x
        )

    def __call__(self, *target_dates: dt.date) -> MarketDataType:
        target_times = list(map(self._target_date_to_time, target_dates))
        yields = list(map(self.get_rate, target_times))
        return list(zip(target_dates, yields))

    def discount_factor(self, target_date: dt.date) -> float:
        """
        Calculate the discount factor for a given target date.

        :param target_date: The target date.
        :type target_date: dt.date
        :return: The discount factor.
        :rtype: float
        """
        target_time = self._target_date_to_time(target_date)
        rate = self.get_rate(target_time)
        return np.exp(-rate * target_time)

    def get_params(self) -> Dict[str, float]:
        """
        Get the parameters of the model.

        :return: a dictionary containing the parameters
        :rtype: Dict[str, float]
        """
        return {
            "beta0": self.beta0,
            "beta1": self.beta1,
            "beta2": self.beta2,
            "beta3": self.beta3,
            "tau1": self.tau1,
            "tau2": self.tau2,
        }
