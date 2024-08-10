#!/usr/bin/env python3

import datetime
import json
import os

from daily.env import MODULE
from daily.market_hrs.base import Equity
from daily.market_hrs.base import EQUITY_OPEN
from daily.market_hrs.base import Option
from daily.market_hrs.base import OPTION_OPEN
from daily.market_hrs.util import time_to_dt
from daily.market_hrs.util import utc_offset


PARENT = os.path.basename(os.path.dirname(__file__))


def equity(
    date: datetime.date = datetime.date.today(),
    closed: bool = False
):
    """
    Generate equity hours json
    """

    if closed:
        from daily.market_hrs.base import EQUITY_CLOSED
        return json.loads(EQUITY_CLOSED.format(date=date))

    prestart: datetime.time | str
    regstart: datetime.time | str
    regend: datetime.time | str
    postend: datetime.time | str

    prestart = input("Enter prestart time (enter to use default):")
    if prestart:
        prestart = datetime.time.fromisoformat(prestart)
    else:
        prestart = Equity.PreMarket.START

    regstart = input("Enter regstart time (enter to use default):")
    if regstart:
        regstart = datetime.time.fromisoformat(regstart)
    else:
        regstart = Equity.RegularMarket.START

    regend = input("Enter regend time (enter to use default):")
    if regend:
        regend = datetime.time.fromisoformat(regend)
    else:
        regend = Equity.RegularMarket.END

    postend = input("Enter postend time (enter to use default):")
    if postend:
        postend = datetime.time.fromisoformat(postend)
    else:
        postend = Equity.PostMarket.END

    return json.loads(
        EQUITY_OPEN.format(
            date=date,
            prestart=prestart,
            regstart=regstart,
            regend=regend,
            postend=postend,
            utc_offset=utc_offset()
        )
    )


def option(
    date: datetime.date = datetime.date.today(),
    closed: bool = False
):
    """
    Generate option hours json
    """

    if closed:
        from daily.market_hrs.base import OPTION_CLOSED
        return json.loads(OPTION_CLOSED.format(date=date))

    regstart: datetime.time | str
    regend: datetime.time | str

    regstart = input("Enter regstart time (enter to use default):")
    if regstart:
        regstart = datetime.time.fromisoformat(regstart)
    else:
        regstart = Option.EQO.RegularMarket.Start

    regend = input("Enter regend time (enter to use default):")
    if regend:
        regend = datetime.time.fromisoformat(regend)
    else:
        regend = Option.EQO.RegularMarket.End

    _dt = datetime.datetime.combine(
        date,
        regend
    )
    regend_ind = (_dt + datetime.timedelta(minutes=15)).time()

    return json.loads(
        OPTION_OPEN.format(
            date=date,
            regstart=regstart,
            regend=regend,
            regend_ind=regend_ind,
            utc_offset=utc_offset()
        )
    )


def get_timedelta_seconds(now=None):

    return (
        datetime.datetime.now() - time_to_dt(Equity.REGEND)
    ).total_seconds() + 60 * 15


def past_regend() -> bool:
    return datetime.datetime.now() >= time_to_dt(Equity.REGEND)


def pre_regstart() -> bool:
    return datetime.datetime.now() < time_to_dt(Equity.REGSTART)


def get_prestart(now: datetime.datetime) -> datetime.time:
    return min(now.time(), Equity.PreMarket.START)


def get_regstart(now: datetime.datetime) -> datetime.time:
    return min(now.time(), Equity.RegularMarket.START)


def get_regend(now: datetime.datetime) -> datetime.time:
    now = now + datetime.timedelta(seconds=60*15)
    return max(now.time(), Equity.RegularMarket.END)


def get_postend(now: datetime.datetime) -> datetime.time:
    now = now + datetime.timedelta(seconds=60*15)
    return max(now.time(), Equity.PostMarket.END)


def smart(to_disk: bool = True):

    date = datetime.date.today()
    utcoffset = utc_offset()
    prestart = Equity.PreMarket.Start
    regstart = Equity.RegularMarket.Start
    regend = Equity.RegularMarket.End
    postend = Equity.PostMarket.End
    regend_ind = Option.IND.RegularMarket.End
    now = datetime.datetime.now()

    if past_regend():
        regend = now + datetime.timedelta(minutes=15)
        regend = datetime.datetime(
            regend.year, regend.month, regend.day,
            regend.hour, regend.minute, regend.second
        )
        postend = (regend + datetime.timedelta(minutes=15)).time()
        regend_ind = (regend + datetime.timedelta(minutes=15)).time()
        regend = regend.time()

    elif pre_regstart():
        prestart = regstart = datetime.datetime(
            now.year, now.month, now.day, now.hour, now.minute, now.second
        ).time()

    equity_json = json.loads(
        EQUITY_OPEN.format(
            date=date,
            prestart=prestart,
            regstart=regstart,
            regend=regend,
            postend=postend,
            utc_offset=utcoffset
        )
    )

    option_json = json.loads(
        OPTION_OPEN.format(
            date=date,
            regstart=regstart,
            regend=regend,
            regend_ind=regend_ind,
            utc_offset=utcoffset
        )
    )

    hrs_json = equity_json | option_json

    if to_disk:
        to = f'./{MODULE}/{PARENT}/market_hours.json'
        with open(to, 'w') as f:
            json.dump(hrs_json, f)
    else:
        from pprint import pprint
        pprint(hrs_json)


def write(
    mkt="both",
    to=f'./{MODULE}/{PARENT}/',
    closed=False,
    smart: bool = False
) -> None:
    """
    Generate and write market hours
    """

    if mkt.lower() == "both":
        hrs_json = equity(closed=closed)
        hrs_json.update(option(closed=closed))
        to += 'market_hours.json'
    elif 'option' in mkt.lower():
        hrs_json = option(closed=closed)
        to += 'market_hours_options.json'
    else:
        hrs_json = equity(closed=closed)
        to += 'market_hours.json'

    with open(to, 'w') as f:
        json.dump(hrs_json, f)
