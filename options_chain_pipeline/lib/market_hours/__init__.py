#!/usr/bin/env python3
import os
# from pathlib import Path

MARKET_HOURS_JSON = "market_hours.json"
MARKET_HOURS_PATH = os.path.join(os.path.dirname(__file__), MARKET_HOURS_JSON)

# if not Path(MARKET_HOURS_PATH).is_file():
#     from .util import find_market_hours_path

#     try:
#         MARKET_HOURS_PATH = find_market_hours_path()
#     except:
#         pass

from .functions import *

del os