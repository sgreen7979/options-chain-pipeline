#!/usr/bin/env python3
import datetime as dt
from typing import Dict
from typing import Optional

import numpy as np
from scipy.optimize import minimize

from daily.term_structure.base import YieldTermStructure
from daily.term_structure.types import MarketDataType
from daily.term_structure.types import NelsonSiegelParamsType


class NelsonSiegelYieldTermStructure(YieldTermStructure):

    default_initial_guess: NelsonSiegelParamsType = [0.03, -0.02, 0.02, 1.0]

    def __init__(
        self,
        reference_date: dt.date,
        data_points: MarketDataType,
        *,
        initial_guess: Optional[NelsonSiegelParamsType] = None,
    ) -> None:
        """
        Initialize Nelson-Siegel Yield Term Structure and calibrate parameters.

        :param reference_date: The reference date for the yield curve.
        :type reference_date: dt.date
        :param data_points: List of tuples with dates and corresponding yields.
        :type data_points: List[Tuple[dt.date, float]]
        """
        super().__init__(reference_date, data_points)
        self.initial_guess = initial_guess or self.default_initial_guess
        self.calibrate()

    def get_rate(self, t: float) -> float:
        """
        Calculate the Nelson-Siegel interest rate for a given time t.

        :param t: Time to maturity in years.
        :type t: float
        :return: The interest rate.
        :rtype: float
        """
        term1 = self.beta0
        term2 = self.beta1 * (1 - np.exp(-t / self.tau)) / (t / self.tau)
        term3 = self.beta2 * (
            (1 - np.exp(-t / self.tau)) / (t / self.tau) - np.exp(-t / self.tau)
        )
        return term1 + term2 + term3

    def objective_ns(self, params: NelsonSiegelParamsType) -> float:
        """
        Objective function for minimizing the difference between observed and model yields.

        :param params: List of parameters [beta0, beta1, beta2, tau].
        :type params: List[float]
        :return: Sum of squared differences between observed and model yields.
        :rtype: float
        """
        self.beta0, self.beta1, self.beta2, self.tau = params
        predicted_yields = list(map(self.get_rate, self.times))
        return float(np.sum((np.array(predicted_yields) - np.array(self.yields)) ** 2))

    def calibrate(self) -> None:
        """
        Calibrate the Nelson-Siegel model parameters.

        :return: Calibrated parameters [beta0, beta1, beta2, tau].
        :rtype: Tuple[float, float, float, float].
        """
        self.beta0, self.beta1, self.beta2, self.tau = tuple(
            minimize(self.objective_ns, self.initial_guess, method='Nelder-Mead').x
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
        rate: float = self.get_rate(target_time)
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
            "tau": self.tau,
        }
