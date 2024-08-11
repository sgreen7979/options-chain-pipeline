#!/usr/bin/env python3
import datetime as dt
from functools import lru_cache
from typing import List
from typing import Optional

from .types import MarketDataType


class SimpleYieldTermStructure:
    def __init__(self, reference_date: dt.date) -> None:
        self.reference_date = reference_date

    def discount_factor(self, target_date: dt.date) -> float:
        raise NotImplementedError("Must implement discount_factor method")

    @lru_cache(maxsize=None)
    def get_rate(self, t: float) -> float:
        raise NotImplementedError("Must implement get_rate method")

    @lru_cache(maxsize=None)
    def _target_date_to_time(self, target_date: dt.date) -> float:
        """
        Converts a target date to a time as a fraction of a year with
        respect to the reference date
        """
        return (target_date - self.reference_date).days / 365.0


class YieldTermStructure(SimpleYieldTermStructure):
    def __init__(self, reference_date: dt.date, data_points: MarketDataType):
        super().__init__(reference_date)
        self.data_points = sorted(data_points)

    @property
    def dates(self) -> List[dt.date]:
        """Returns a list of the dates in data points"""
        return [d for d, _ in self.data_points]

    @property
    def times(self) -> List[float]:
        """Returns a list of the times in data points as fractions of a year"""
        return list(map(self._target_date_to_time, self.dates))

    @property
    def yields(self) -> List[float]:
        """Returns a list of the yield rates in data_points"""
        return [dp[1] for dp in self.data_points]

    @classmethod
    def without_data(cls, ref_date: Optional[dt.date] = None):
        from .market_data import get_market_data

        ref_date, mkt_data = get_market_data()
        return cls(ref_date, mkt_data)
