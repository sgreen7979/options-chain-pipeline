#!/usr/bin/env python3
from collections import namedtuple
import datetime as dt
import time


class UtcOffset:
    def __init__(self):
        self._utc_offset_sec = self.get_utc_offset_sec()

    @staticmethod
    def get_utc_offset_sec() -> int:
        return time.localtime().tm_gmtoff

    @property
    def timezone(self) -> str:
        return time.localtime().tm_zone

    @property
    def sign(self) -> str:
        """
        "-" if offset is negative, else "+"
        """
        return "-" if self.utc_offset_sec < 0 else "+"

    @property
    def seconds(self) -> int:
        return self._utc_offset_sec

    @property
    def minutes(self) -> float:
        return self.seconds / 60

    @property
    def hours(self) -> float:
        return self.seconds / 3600

    @property
    def parts(self):

        Parts = namedtuple("Parts", "days hours minutes seconds microseconds")
        # seconds = abs(self.seconds)
        # minutes = abs(self.minutes)
        hours = self.hours
        sign = -1 if hours < 0 else 1
        hours = abs(hours)
        days = 0
        while hours >= 24:
            days += 1
            hours -= 24
            # minutes -= 24 * 60
            # seconds -= 24 * 60 * 60
        days *= sign
        minutes = (hours - int(hours)) * 60
        hours = sign * int(hours)
        seconds = (minutes - int(minutes)) * 60
        minutes = sign * int(minutes)
        microseconds = int((seconds - int(seconds)) * 1e6 * sign)
        seconds = sign * int(seconds)
        return Parts(days, hours, minutes, seconds, microseconds)

    utc_offset_sec = seconds  # alias for backwards compatibility

    @property
    def timedelta(self) -> dt.timedelta:
        return dt.timedelta(seconds=self.seconds)

    def toUtc(self, naive: dt.datetime = dt.datetime.now()) -> dt.datetime:
        return naive - self.timedelta

    def localize(self, naive: dt.datetime = dt.datetime.now()) -> dt.datetime:
        return naive + self.timedelta

    def utc_offset_hr_float_abs(self):
        return abs(self._utc_offset_hr_float())

    def _utc_offset_hr_float(self):
        return self.utc_offset_sec / 60 / 60

    def utc_offset_hr_float_OLD(self):
        return abs(self.utc_offset_sec / 60 / 60)

    @property
    def utc_offset_hr(self):
        # return int(self.utc_offset_hr_float_OLD())
        return int(self._utc_offset_hr_float())

    @property
    def utc_offset_min(self):
        # return int(self.utc_offset_hr_float_OLD() % 1 * 60)
        return int(self._utc_offset_hr_float() % 1 * 60)

    def stringify(self):
        # return f"{self.sign}{self.utc_offset_hr:02d}:{self.utc_offset_min:02d}"
        utc_offset_hr = abs(self.utc_offset_hr)
        utc_offset_min = abs(self.utc_offset_min)
        return f"{self.sign}{utc_offset_hr:02d}:{utc_offset_min:02d}"

    __str__ = stringify

    def __repr__(self):
        return f"{type(self).__name__}<{self.timezone}[{self.__str__()}]>"
