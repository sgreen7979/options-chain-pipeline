#!/usr/bin/env python3

import datetime as dt
from pathlib import Path
from typing import Optional

from ..market_hours import MARKET_HOURS_JSON

TODAY = datetime.date.today()
root = Path(__file__).parent


def strptime(dt_str: str) -> dt.datetime:
    return dt.datetime.strptime(dt_str, f"%Y-%m-%dT%H:%M:%S{utc_offset_str()}")


def search_module_path() -> list[Path]:
    return list(Path(__file__).parent.glob("market_hours.json"))


def search_cwd() -> list[Path]:
    return list(Path().cwd().glob("**/market_hours.json"))


def find_market_hours_paths() -> list[Path]:
    module_matches = search_module_path()
    # cwd_matches = list(Path().cwd().glob(f"**/{filename}"))
    return module_matches + [p for p in search_cwd() if p not in module_matches]


def find_market_hours_path() -> Path:
    return find_market_hours_paths()[0]
