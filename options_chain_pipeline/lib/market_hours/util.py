#!/usr/bin/env python3

import datetime
from pathlib import Path
import time
from typing import Optional

from daily.types import StrPath
from daily.utils.cal import UtcOffset

TODAY = datetime.date.today()
root = Path(__file__).parent


def utc_offset_str() -> str:
    """
    Get utc offset str in hours (+/-HH:SS)
    e.g., "-05:30", "+03:00"
    """
    # utc_offset_sec = time.localtime().tm_gmtoff
    utc_offset_sec = UtcOffset().seconds
    utc_offset_hr_float = abs(utc_offset_sec / 60 / 60)
    utc_offset_hr = int(utc_offset_hr_float)
    utc_offset_min = int(utc_offset_hr_float % 1 * 60)

    if utc_offset_sec < 0:
        return f"-{utc_offset_hr:02d}:{utc_offset_min:02d}"
    else:
        return f"+{utc_offset_hr:02d}:{utc_offset_min:02d}"


utc_offset = utc_offset_str


def hours_to_datetime(dt: str) -> datetime.datetime:
    return datetime.datetime.strptime(dt, f"%Y-%m-%dT%H:%M:%S{utc_offset_str()}")


def time_to_dt(time: datetime.time, date: datetime.date = TODAY) -> datetime.datetime:
    return datetime.datetime.combine(date=date, time=time)


def get_dirs(root: Optional[StrPath] = None):
    root = Path(root or "*")
    if root.is_dir():
        dirs = []
        for path in root.iterdir():
            if path.is_dir():
                dirs.append(path)
                dirs += get_dirs(path) or []
        return iter(dirs)


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
