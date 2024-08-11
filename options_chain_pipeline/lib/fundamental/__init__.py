#!/usr/bin/env python3
import os

from daily.env import DATA_PATH

__all__ = [
    "FUNDAMENTAL_DATA_PATH",
    "FUNDAMENTAL_DB_PATH",
    "METADATA_PATH"
]

FUNDAMENTAL_DATA_PATH = os.path.join(DATA_PATH, "fundamental")
FUNDAMENTAL_DB_PATH = os.path.join(FUNDAMENTAL_DATA_PATH, "fundamental.json")
METADATA_PATH = os.path.join(FUNDAMENTAL_DATA_PATH, "fundamental.pickle")

del os
del DATA_PATH
