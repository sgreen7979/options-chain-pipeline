#!/usr/bin/env python3
import datetime as dt
from typing import cast

from scipy.interpolate import CubicSpline

from .base import YieldTermStructure
from .types import MarketDataType


class CubicSplineYieldTermStructure(YieldTermStructure):
    def __init__(self, reference_date: dt.date, data_points: MarketDataType) -> None:
        super().__init__(reference_date, data_points)
        self.spline: CubicSpline = CubicSpline(self.times, self.yields)

    def get_rate(self, t: float) -> float:
        if t <= self.spline.x[0]:
            rate = self.spline(self.spline.x[0])
        elif t >= self.spline.x[-1]:
            rate = self.spline(self.spline.x[-1])
        else:
            rate = self.spline(t)
        return float(rate)

    def __call__(self, *target_dates: dt.date) -> MarketDataType:
        target_times = list(map(self._target_date_to_time, target_dates))
        yields = list(map(self.get_rate, target_times))
        return list(zip(target_dates, yields))

    def discount_factor(self, target_date: dt.date) -> float:
        """Calculate the discount factor for a given target date.

        :param target_date: the target date
        :type target_date: dt.date
        :return: the discount factor
        :rtype: float
        """
        target_time = self._target_date_to_time(target_date)
        rate = self.get_rate(target_time)
        return cast(float, 1 / (1 + rate * target_time))
