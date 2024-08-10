#!/usr/bin/env python3
import datetime as dt

from .base import YieldTermStructure
from .types import MarketDataType


class PiecewiseLinearYieldTermStructure(YieldTermStructure):
    def __init__(self, reference_date: dt.date, data_points: MarketDataType) -> None:
        super().__init__(reference_date, data_points)

    def get_rate(self, t: float) -> float:
        if t <= self.times[0]:
            rate = self.yields[0]
        elif t >= self.times[-1]:
            rate = self.yields[-1]
        else:
            for i in range(1, len(self.times)):
                if t < self.times[i]:
                    t1, r1 = self.times[i - 1], self.yields[i - 1]
                    t2, r2 = self.times[i], self.yields[i]
                    rate = r1 + (r2 - r1) * (t - t1) / (t2 - t1)
                    break
        return rate

    def __call__(self, *target_dates: dt.date) -> MarketDataType:
        target_times = list(map(self._target_date_to_time, target_dates))
        yields = list(map(self.get_rate, target_times))
        return list(zip(target_dates, yields))

    def discount_factor(self, target_date: dt.date) -> float:
        target_time = self._target_date_to_time(target_date)
        rate = self.get_rate(target_time)
        return 1 / (1 + rate * target_time)
