#!/usr/bin/env python3
import datetime as dt

from .base import SimpleYieldTermStructure
from .types import MarketDataType


class FlatYieldTermStructure(SimpleYieldTermStructure):
    def __init__(self, reference_date: dt.date, rate: float) -> None:
        super().__init__(reference_date)
        self.rate = rate

    def get_rate(self, t: float) -> float:
        return self.rate

    def __call__(self, *target_dates: dt.date) -> MarketDataType:
        target_times = list(map(self._target_date_to_time, target_dates))
        yields = [self.rate for _ in target_times]
        return list(zip(target_dates, yields))

    def discount_factor(self, target_date: dt.date) -> float:
        t = self._target_date_to_time(target_date)
        return 1 / (1 + self.rate * t)
