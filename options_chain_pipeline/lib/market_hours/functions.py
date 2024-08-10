#!/usr/bin/env python3
import datetime as dt
import json
import os
import re
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import TYPE_CHECKING

from ..market_hours import MARKET_HOURS_PATH
from .util import strptime

if TYPE_CHECKING:
    from daily.schwab.client import SchwabClient

MARKETS = ["EQUITY", "OPTION", "FUTURE", "BOND", "FOREX"]

MarketsType = Literal[
    "EQUITY",
    "OPTION",
    "FUTURE",
    "BOND",
    "FOREX",
    "equity",
    "option",
    "future",
    "bond",
    "forex",
]


def read():
    if os.path.exists(MARKET_HOURS_PATH):
        with open(MARKET_HOURS_PATH, "r") as f:
            data = json.load(f)
        return data


def write(data: dict) -> None:
    with open(MARKET_HOURS_PATH, "w") as f:
        json.dump(data, f, indent=4)


def get_date(hours: Dict) -> dt.date:
    if "date" in hours:
        return dt.date.fromisoformat(hours["date"])
    else:
        market_key = next(k for k in hours.keys())
        hours = hours[market_key]
        subkey = next(k for k in hours.keys())
        hours = hours[subkey]
        return dt.date.fromisoformat(hours["date"])


def isOpen(hours: Dict, market: Optional[MarketsType] = None) -> bool:
    if market is None and "isOpen" in hours:
        return hours["isOpen"]
    elif market is not None:
        market_key = market.lower()
    else:
        market_key = next(k for k in hours.keys() if k != "forex")
    hours = hours[market_key]
    subkey = next(k for k in hours.keys())
    hours = hours[subkey]
    return hours["isOpen"]


def _split_xpath(xpath: str):
    """
    e.g., `'$.option.EQO.sessionHours.regularMarket[0].start'`

    note:   `'regularMarket[0]'` means `dict["regularMarket"][0]`
    """
    list_index_pat = re.compile(r"(\[\d+\])$")
    xpath_split = xpath.split(".")
    xpath_decoded = []
    for p in xpath_split:
        paths = list_index_pat.split(p)
        paths = list(i for i in paths if i and i != "$")
        xpath_decoded.extend(paths)
    return xpath_decoded


def _xpath_parse(hours, xpath):
    """
    e.g.::

        >>> xpath = '$.option.EQO.sessionHours.regularMarket[0].start'
        >>> xpath = '$.equity.EQ.isOpen'
    """
    xpath_decoded = _split_xpath(xpath)
    position = hours
    for p in xpath_decoded:
        if p.startswith("[") and p.endswith("]"):
            p = int(p.replace("[", "").replace("]", ""))
        position = position[p]
    return position


def get_hour(hours, xpath):
    """
    "$.option.EQO.sessionHours.regularMarket[0].start"
    "$.option.IND.sessionHours.regularMarket[0].end"
    "$.equity.EQ.sessionHours.regularMarket[0].end"
    "$.equity.EQ.sessionHours.preMarket[0].start"
    """
    datetime_string = _xpath_parse(hours, xpath)
    return strptime(datetime_string)


def get_markets(hours: Dict) -> List[str]:
    return list(map(str.upper, list(hours.keys())))


def fetch(
    client: Optional["SchwabClient"] = None,
    date: Optional[dt.date] = None,
    markets: Optional[List[str]] = None,
    *,
    save: bool = True,
) -> dict:
    if date is None:
        date = dt.date.today()

    if markets is None:
        markets = MARKETS
    else:
        markets = list(map(str.upper, markets))

    hours = read()
    if hours and get_date(hours) == date and all(m.lower() in hours for m in markets):
        return hours
    else:
        if client is None:
            from daily.schwab.client import ClientGroup

            client = ClientGroup()._await_client_with_capacity(incl_queued=True)
        # client._await_capacity(incl_queued=True)
        hours = client.get_market_hours(markets, date=date.isoformat())
        hours["date"] = date.isoformat()
        hours["isOpen"] = isOpen(hours)
        if save:
            write(hours)
    return hours


def fetch_today(
    client: Optional["SchwabClient"] = None,
    markets: Optional[List[str]] = None,
) -> dict:
    return fetch(client, markets=markets)


def fetch_today_and_parse(
    client: Optional["SchwabClient"] = None,
    markets: Optional[List[str]] = None,
    *xpaths,
):
    hours = fetch_today(client, markets)

    def try_strptime(v):
        if not isinstance(v, str):
            return v
        try:
            return strptime(v)
        except Exception:
            return v

    from itertools import repeat

    parsed = list(map(_xpath_parse, repeat(hours), xpaths))
    return [try_strptime(i) for i in parsed]
