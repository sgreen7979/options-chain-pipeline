#!/usr/bin/env python3
import os

from options_chain_pipeline.lib.env import DATA_PATH

__all__ = [
    "FUNDAMENTAL_DATA_PATH",
    "FUNDAMENTAL_DB_PATH",
    "METADATA_PATH",
    "FundamentalsLoader"
]

FUNDAMENTAL_DATA_PATH = os.path.join(DATA_PATH, "fundamental")
os.makedirs(FUNDAMENTAL_DATA_PATH, exist_ok=True)
FUNDAMENTAL_DB_PATH = os.path.join(FUNDAMENTAL_DATA_PATH, "fundamental.json")
METADATA_PATH = os.path.join(FUNDAMENTAL_DATA_PATH, "fundamental.pickle")

from .loader import FundamentalsLoader

del os
del DATA_PATH
