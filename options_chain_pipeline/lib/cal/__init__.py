#!/usr/bin/env python3

import datetime
import time
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from .utc import UtcOffset

US_BUSINESS_DAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())

__all__ = [
    "is_bizday",
    "get_prior_bizday",
]


def is_bizday(dt: Optional[Union[datetime.date, datetime.datetime]] = None) -> bool:
    """Check if dt represents a U.S. business day.

    Args:
        date (datetime.datetime | datetime.date, optional):
            The date or datetime to check.
            Defaults to `datetime.date.today()`.

    Returns:
        bool:
            Whether date is a business day.
    """
    if dt is not None:
        if isinstance(dt, datetime.datetime):
            start = cast(dt, datetime.date)
        else:
            start = dt
    else:
        start = datetime.date.today()
    return get_bizday_from_offset(0, start=start) == start


def get_bizday_from_offset(
    ndays: int, start: Optional[Union[datetime.datetime, datetime.date]] = None
) -> datetime.date:
    """Get the business day that is ndays away from the start datetime.

    Args:
        ndays (int):
            The number of days offset to the start datetime.

        start (datetime.datetime | datetime.date, optional):
            The start date or datetime.
            Defaults to `datetime.datetime.now()`.

    Returns:
        datetime.date:
            The business day.

    """
    start = cast(start or datetime.datetime.now(), datetime.datetime)
    result = start + ndays * US_BUSINESS_DAY
    return result.date() if isinstance(result, datetime.datetime) else result


def get_prior_bizday(
    ndays: int = 1, start: Optional[Union[datetime.datetime, datetime.date]] = None
) -> datetime.date:
    """Get the business day that is ndays before the start datetime.

    Args:
        ndays (int, optional):
            Number of days to subtract from the start datetime.
            Defaults to 1.

        start (datetime.datetime, optional):
            The start datetime.
            Defaults to `datetime.datetime.now()`.

    Returns:
        datetime.date: prior business day

    """
    return get_bizday_from_offset(ndays=-ndays, start=start)


def cast(
    dt: Union[datetime.datetime, datetime.date],
    dtype: type[Union[datetime.datetime, datetime.date]],
) -> Union[datetime.datetime, datetime.date]:
    """
    Cast a datetime.date object to a datetime.datetime object or
    vice versa.

    Args:
        dt (datetime.datetime | datetime.date):
            The date or datetime object to cast.

        dtype (type[datetime.datetime | datetime.date]):
            Cast dt to this type.

    Returns:
        datetime.datetime | datetime.date:
            The casted object.
    """
    if not isinstance(dt, (datetime.date, datetime.datetime)):
        raise TypeError(
            "dt must be type datetime.datetime or datetime.date, "
            f"got {dt.__class__.__name__}"
        )

    if dtype not in (datetime.date, datetime.datetime):
        raise TypeError(
            "dtype must be datetime.datetime or datetime.date, " f"got {dtype.__name__}"
        )

    if isinstance(dt, dtype):
        return dt
    elif isinstance(dt, datetime.date):
        return convert_date_to_datetime(dt)
    else:
        return dt.date()
