#!/usr/bin/env python3
import itertools
import functools
import math
from typing import Iterable
from typing import List
from typing import Optional

from daily.env import MODULE
from daily.schwab.streaming import constants
from daily.schwab.credentials import NUM_ACCOUNTS
from daily.types import StrPath

from .universe import CATSymbols

body_too_big = [
    "$NDX",
    "$SPX",
    "SPY",
    "$XSP",
]
"""
2024-10-13 21:11:28,809 ERROR daily.utils.logging Request error {"fault":{"faultstring":"Body buffer overflow","detail":{"errorcode":"protocol.http.TooBigBody"}},"params":{"symbol": "$NDX", "contractType": "ALL", "includeUnderlyingQuote": "True", "strategy": "ANALYTICAL", "range": "ALL", "expMonth": "ALL", "optionType": "ALL"},"endpoint":"/marketdata/v1/chains"}
"""

bad_param = [
    "$AAN",
    "$ALLG",
    "$ASLN",
    "$AUGX",
    "$BFA",
    "$BFB",
    "$BRKB",
    "BRK.B",
    "$CALT",
    "CHUY",
    "$CRDA",
    "$CWENA",
    "$DOMA",
    "$EFUT",
    "$GTHX",
    "$HA",
    "$HEIA",
    "HEI.A",
    "$LGFA",
    "$LGFB",
    "$MACK",
    "$MOGA",
    "$MOR",
    "$NSTGQ",
    "$OLK",
    "$PBRA",
    "$PRFT",
    "$PWSC",
    "$TELL",
    "$UHALB",
    "$VGR",
    "$WKME",
]


@functools.lru_cache(maxsize=5)
# def get_all_symbols(filepath: Optional[StrPath] = None) -> Iterable[str]:
def get_all_symbols(filepath: Optional[StrPath] = None) -> List[str]:
    filepath = filepath or f'./{MODULE}/symbols/symbols.txt'
    with open(filepath, 'r') as f:
        symbols = list(map(str.strip, f.readlines()))
        # symbols = [line.strip() for line in f.readlines()]
    return symbols


# @functools.lru_cache(maxsize=None)
def get_options_universe(adapt_to_schwab: bool = True) -> List[str]:
    options = CATSymbols().get_options(to_list=True)
    if not isinstance(options, list):
        raise TypeError(
            "Options retrieved must be type list, got" f"{options.__class__.__name__}"
        )

    if adapt_to_schwab:
        index_options = get_index_options_universe(
            options_universe=options, adapt_to_schwab=False
        )
        for index_option in index_options:
            options[options.index(index_option)] = "$" + index_option
        options = [o.replace(" ", ".") for o in options]

    options = [o for o in options if o not in bad_param]
    return options


# @functools.lru_cache(maxsize=None)
def get_equities_universe(adapt_to_schwab: bool = True) -> List[str]:
    equities = CATSymbols().get_equities(to_list=True)
    if not isinstance(equities, list):
        raise TypeError(
            "Options retrieved must be type list, got" f"{equities.__class__.__name__}"
        )
    if adapt_to_schwab:
        equities = [e.replace(" ", ".") for e in equities]
    return equities


def get_index_options_universe(
    options_universe: Optional[List[str]] = None,
    equities_universe: Optional[List[str]] = None,
    adapt_to_schwab: bool = True,
) -> List[str]:
    ret = list(
        set(options_universe or get_options_universe(False))
        - set(equities_universe or get_equities_universe(False))
    )
    if adapt_to_schwab:
        # ret = ["$" + io.replace(" ", ".") for io in ret]
        ret = list(map(lambda io: "$" + io.replace(" ", "."), ret))

    return ret


def get_equity_options_universe(adapt_to_schwab: bool = True) -> List[str]:
    options_universe = set(get_options_universe(False))
    equities_universe = set(get_equities_universe(False))
    ret = list(options_universe.intersection(equities_universe))
    if adapt_to_schwab:
        ret = list(map(lambda eo: eo.replace(" ", "."), ret))
    return ret


@functools.lru_cache(maxsize=None)
def get_chunks() -> List[List[str]]:
    no_chunks = NUM_ACCOUNTS
    # symbols = get_all_symbols()
    symbols = get_options_universe()
    chunksize = math.ceil(len(symbols) / no_chunks)
    return [symbols[i * chunksize : (i + 1) * chunksize] for i in range(no_chunks)]


def get_chunk(num: int) -> List[str]:
    if 0 < num <= NUM_ACCOUNTS:
        chunks = get_chunks()
        return chunks[num - 1]
    raise ValueError(f"num must be between 1 and {NUM_ACCOUNTS}")


def get_symbol_chunk_number(symbol: str):
    chunks = get_chunks()
    for idx, chunk in enumerate(chunks):
        if symbol in chunk:
            return idx + 1


def rest_syms_args():
    chunks = get_chunks()
    return itertools.cycle(chunks)


def stream_syms_args(syms: Optional[Iterable[str]] = None):
    """
    Limited to 200 tickers per account
    """
    syms = syms or get_all_symbols()
    capacity = NUM_ACCOUNTS * constants.SCHWAB_MAX_STREAMING_SYMBOLS
    syms = list(itertools.islice(syms, capacity))
    chunksize = math.ceil(len(syms) / NUM_ACCOUNTS)
    chunks = [syms[i * chunksize : (i + 1) * chunksize] for i in range(NUM_ACCOUNTS)]
    return itertools.cycle(chunks)
