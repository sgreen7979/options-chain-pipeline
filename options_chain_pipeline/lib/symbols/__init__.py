#!/usr/bin/env python3
from typing import List

from .cat import CATSymbols

__all__ = [
    "CATSymbols",
    "get_options_universe",
    "get_equities_universe",
]


def get_options_universe() -> List[str]:
    return CATSymbols().get_options(to_list=True)


def get_equities_universe() -> List[str]:
    return CATSymbols().get_equities(to_list=True)
