#!/usr/bin/env python3
from typing import List

from .cat import CATSymbols

__all__ = [
    "CATSymbols",
    "get_options_universe",
    "get_equities_universe",
]


def get_options_universe() -> List[str]:
    options = CATSymbols().get_options(to_list=True)
    if not isinstance(options, list):
        raise TypeError(
            "Options retrieved must be type list, got" f"{options.__class__.__name__}"
        )
    return options


def get_equities_universe() -> List[str]:
    equities = CATSymbols().get_equities(to_list=True)
    if not isinstance(equities, list):
        raise TypeError(
            "Options retrieved must be type list, got" f"{equities.__class__.__name__}"
        )
    return equities
