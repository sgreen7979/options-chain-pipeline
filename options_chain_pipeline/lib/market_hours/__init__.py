#!/usr/bin/env python3
# import datetime
import os
from pathlib import Path

# from requests import Response
# from typing import List
# from typing import Literal

# from daily.env import MODULE
# from daily import PROJECT_ROOT
# from daily.tda.accounts import get_credentials

MARKET_HOURS_JSON = "market_hours.json"
# MARKET_HOURS_PATH = f"./{MODULE}/{Path(__file__).parent.name}/{MARKET_HOURS_JSON}"
MARKET_HOURS_PATH = os.path.join(os.path.dirname(__file__), MARKET_HOURS_JSON)
# os.path.join(PROJECT_ROOT, )

if not Path(MARKET_HOURS_PATH).is_file():
    from daily.market_hrs.util import find_market_hours_path

    MARKET_HOURS_PATH = find_market_hours_path()


# ####################################################################################################################################
# ####################################################################################################################################
# # NOT IMPLEMENTED
# ####################################################################################################################################
# ####################################################################################################################################
# from tda.auth import easy_client
# from tda.client import Client as TDClient

# _name_to_member = {
#     "BOND": TDClient.Markets.BOND,
#     "EQUITY": TDClient.Markets.EQUITY,
#     "FOREX": TDClient.Markets.FOREX,
#     "FUTURE": TDClient.Markets.FUTURE,
#     "OPTION": TDClient.Markets.OPTION,
# }
# _member_to_name = {
#     TDClient.Markets.BOND: "BOND",
#     TDClient.Markets.EQUITY: "EQUITY",
#     TDClient.Markets.FOREX: "FOREX",
#     TDClient.Markets.FUTURE: "FUTURE",
#     TDClient.Markets.OPTION: "OPTION",
# }


# class _MarketHours:

#     def __init__(self, decoder=None) -> None:
#         self.decoder = decoder
#         self._resp = self._get_todays_hours(1, ["EQUITY", "OPTION"])

#     def _get_todays_hours(
#         self,
#         acct_idx: int,
#         markets: List[Literal["BOND", "EQUITY", "FOREX", "FUTURE", "OPTION"]],
#     ) -> Response:
#         """
#         fetch today's market hours for
#         equites, equity options and index options
#         """

#         credentials = get_credentials(acct_idx)
#         markets_enums = [_name_to_member[market_name] for market_name in markets]

#         webdriver_func = None
#         if not os.path.isfile(credentials.token_path):
#             from daily.tda.util import chromedriver as webdriver_func

#         client = easy_client(
#             credentials.api_key,
#             credentials.redirect_uri,
#             credentials.token_path,
#             webdriver_func=webdriver_func,
#         )
#         today = datetime.date.today()
#         resp = client.get_hours_for_multiple_markets(markets_enums, date=today)
#         resp.raise_for_status()
#         return resp
#         # self._hours = resp.json()
#         # for market_name in markets:
#         # setattr(self, market_name, self._hours[market_name])
#         # return self

#     def to_dict(self):
#         return self._resp.json()

#     class Session:
#         class Bond:
#             pass

#         class Equity:
#             pass

#         class Forex:
#             pass

#         class Future:
#             pass

#         class Option:
#             pass

#     def to_session(self):
#         resp_data = self.to_dict()


# # def _convert_enum_iterable(iterable):
# #     required_enum_type = TDClient.Markets

# #     if isinstance(iterable, required_enum_type):
# #         return [iterable.value]

# #     values = []
# #     for value in iterable:
# #         if isinstance(value, required_enum_type):
# #             values.append(value.value)
# #         else:
# #             values.append(value)
# #     return values
