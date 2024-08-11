#!/usr/bin/env python3
import datetime as dt
from pathlib import Path
from typing import Optional

from options_chain_pipeline.lib.cal import UtcOffset


def strptime(dt_str: str) -> dt.datetime:
    return dt.datetime.strptime(dt_str, f"%Y-%m-%dT%H:%M:%S{str(UtcOffset())}")


def search_module_path() -> list[Path]:
    from ..market_hours import MARKET_HOURS_JSON
    return list(Path(__file__).parent.glob(MARKET_HOURS_JSON))


def search_cwd() -> list[Path]:
    from ..market_hours import MARKET_HOURS_JSON
    return list(Path().cwd().glob(f"**/{MARKET_HOURS_JSON}"))


def find_market_hours_paths() -> list[Path]:
    module_matches = search_module_path()
    # cwd_matches = list(Path().cwd().glob(f"**/{filename}"))
    return module_matches + [p for p in search_cwd() if p not in module_matches]


def find_market_hours_path() -> Path:
    return find_market_hours_paths()[0]
