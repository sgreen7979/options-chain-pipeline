#!/usr/bin/env python3
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from functools import lru_cache
from typing import Optional
from tzlocal import get_localzone

import pandas as pd
import pandas_market_calendars as mcal

from options_chain_pipeline.lib.cal import get_prior_bizday

from .mcal.calendars import MarketCalendars


class MarketCalendar:
    """
    A handler for managing market calendars, calculating trading hours, and
    converting dates.

    Attributes:
        end_date_default_timedelta (dt.timedelta): Default time delta for end date calculation.
        TRADING_HOURS_PER_DAY (float): Number of trading hours in a day.
        TRADING_SECONDS_PER_DAY (float): Number of trading seconds in a day.
        TRADING_DAYS_PER_YEAR (int): Number of trading days in a year.
        TRADING_HOURS_PER_YEAR (float): Number of trading hours in a year.
    """

    end_date_default_timedelta: timedelta = timedelta(days=365.25 * 4)
    TRADING_HOURS_PER_DAY: float = 6.5
    TRADING_SECONDS_PER_DAY: float = TRADING_HOURS_PER_DAY * 3600
    TRADING_DAYS_PER_YEAR: int = 252
    TRADING_HOURS_PER_YEAR: float = TRADING_HOURS_PER_DAY * TRADING_DAYS_PER_YEAR
    TRADING_SECONDS_PER_YEAR: float = TRADING_SECONDS_PER_DAY * TRADING_DAYS_PER_YEAR

    def __init__(
        self,
        name: MarketCalendars = MarketCalendars.NYSE,
        start: Optional[date] = None,
        current_time: Optional[datetime] = None,
    ) -> None:
        """Initialize the MarketCalendar with a specific market calendar.

        :param name: The market calendar to use, defaults to MarketCalendars.NYSE
        :type name: MarketCalendars, optional
        """
        self.name = name.value
        self.set_current_time(current_time or self.now())
        if start is not None:
            self.START = start
        elif current_time is not None:
            self.START = current_time.date()
        else:
            self.START = get_prior_bizday()

        self.END = (self.current_time + self.end_date_default_timedelta).date()
        self.tz = get_localzone().key
        self.schedule = self._get_schedule()
        self.remaining_time_today = self._get_remaining_time_today()

    @property
    def calendar(self) -> mcal.MarketCalendar:
        return mcal.get_calendar(self.name)

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    @property
    def current_start_date(self) -> date:
        return self.current_time.date()

    def _get_schedule(self) -> pd.DataFrame:
        """
        Generate the market schedule, including trading hours and cumulative
        trading days.

        :return: The market schedule.
        :rtype: DataFrame
        """
        schedule = self.calendar.schedule(
            start_date=self.START, end_date=self.END, tz=self.tz
        )
        schedule.reset_index(inplace=True)
        schedule.rename(columns={'index': 'datetime'}, inplace=True)
        schedule['iso_date'] = schedule['datetime'].dt.strftime('%Y-%m-%d')
        schedule["hours"] = schedule.apply(
            lambda row: self._calculate_hours_single_row(row), axis=1
        )

        cutoff_idxs = schedule[
            schedule["iso_date"] <= date.today().isoformat()
        ].index.tolist()

        if not cutoff_idxs:
            schedule['T'] = schedule['hours'].cumsum() / self.TRADING_HOURS_PER_YEAR
        else:
            cutoff_idx = cutoff_idxs[-1]
            first_rows = schedule.iloc[: cutoff_idx + 1]  # NEW
            cumsum_rest = (
                schedule.iloc[cutoff_idx + 1 :]["hours"].cumsum()
                / self.TRADING_HOURS_PER_YEAR
            )
            cumsum_series = pd.concat(
                [
                    pd.Series(
                        [None for _ in range(cutoff_idx + 1)], index=first_rows.index
                    ),
                    cumsum_rest,
                ]
            )

            schedule['T'] = cumsum_series  # NEW

        schedule = schedule[
            ["iso_date", "market_open", "market_close", "T", "hours", "datetime"]
        ]
        schedule.set_index('iso_date', inplace=True)

        return schedule

    def _calculate_hours_single_row(self, row) -> float:
        row_date = row['datetime'].date()
        row_open = row['market_open']
        row_close = row['market_close']
        if row_date > self.current_start_date:
            return (row_close - row_open).total_seconds() / 3600
        elif row_date == self.current_start_date:
            start = max(self.current_time.replace(tzinfo=row_open.tzinfo), row_open)
            end = row_close
            return max((end - start).total_seconds() / 3600, 0)
        else:
            return 0.0

    @lru_cache(None)
    def _dte_to_t(self, dte: int) -> float:
        if dte <= 0:
            return 0.0
        expiry = self._expiry_from_dte(dte)
        if expiry <= self.START:
            return 0.0
        return self.schedule.loc[expiry.isoformat()]["T"]

    def _get_remaining_time_today(
        self,
        regend: datetime = datetime.combine(date.today(), time(16, 0, 0, 0)),
    ) -> float:
        if self.current_time.date().isoformat() not in self.schedule.index:
            return 0.0
        if self.current_time >= regend:
            return 0.0
        remaining_seconds = (regend - self.current_time).total_seconds()
        # return remaining_seconds / self.TRADING_SECONDS_PER_DAY
        return remaining_seconds / self.TRADING_SECONDS_PER_YEAR

    def __call__(self, dte: int) -> float:
        return self.get_t(dte)

    def get_t(self, dte: int) -> float:
        return self._dte_to_t(dte) + self.remaining_time_today

    def _expiry_from_dte(self, dte: int) -> date:
        return (self.current_time + timedelta(days=dte)).date()

    # def update(self, current_time: Optional[datetime] = None) -> None:
    #     self.set_current_time(current_time or self.now())
    #     # self.schedule['hours'] = self._recalculate_hours()
    #     # self.schedule['T'] = (
    #     #     self.schedule['hours'].cumsum() / self.TRADING_HOURS_PER_YEAR
    #     # )

    # def _recalculate_hours(self) -> Series:
    #     # current_time_date = self.current_time.date()

    #     # future_mask = self.schedule['datetime'].dt.date > current_time_date
    #     # current_mask = self.schedule['datetime'].dt.date == current_time_date

    #     return self.schedule.apply(
    #         lambda row: self._calculate_hours_single_row(row), axis=1
    #     )

    #     # with ThreadPoolExecutor() as executor:
    #     #     futures = {
    #     #         executor.submit(self._calculate_hours_single_row, row): idx
    #     #         for idx, row in self.schedule.iterrows()
    #     #     }
    #     #     hours = Series(index=self.schedule.index, dtype=float)
    #     #     for future in as_completed(futures):
    #     #         idx = futures[future]
    #     #         hours.at[idx] = future.result()

    #     # return hours

    def set_current_time(self, current_time: datetime) -> "MarketCalendar":
        self.current_time = current_time
        if hasattr(self, "schedule"):
            self.remaining_time_today = self._get_remaining_time_today()
        return self

    def __repr__(self) -> str:
        return f"{type(self).__name__}<name='{self.name}', START={self.START.isoformat()}, END={self.END.isoformat()}>"
