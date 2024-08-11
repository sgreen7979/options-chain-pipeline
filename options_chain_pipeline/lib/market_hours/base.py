#!/usr/bin/env python3

from datetime import date
from datetime import datetime
from datetime import time

TODAY = date.today()


def today_at(t: time) -> datetime:
    return datetime.combine(TODAY, time)


class Option:
    class Equity:
        class RegularMarket:
            Start = time(9, 30)
            End = time(16, 0)
            start = START = Start
            end = END = End
        regularMarket = REG = RegularMarket

    class Index:
        class RegularMarket:
            Start = time(9, 30)
            End = time(16, 15)
            start = START = Start
            end = END = End
        regularMarket = REG = RegularMarket

    EQO = Equity
    IND = Index


class Equity:

    class PreMarket:
        Start = time(7, 0)
        End = time(9, 30)
        start = START = Start
        end = END = End

    class RegularMarket:
        Start = time(9, 30)
        End = time(16, 0)
        start = START = Start
        end = END = End

    class PostMarket:
        Start = time(16, 0)
        End = time(20, 0)
        start = START = Start
        end = END = End

    regularMarket = REG = RegularMarket
    preMarket = PRE = PreMarket
    postMarket = POST = PostMarket
    prestart = PRESTART = PreMarket.Start
    regstart = REGSTART = RegularMarket.Start
    regend = REGEND = RegularMarket.End
    postend = POSTEND = PostMarket.End


class MarketHours:
    Simple = list(map(
        today_at, [
            Equity.PreMarket.Start,
            Equity.RegularMarket.Start,
            Equity.RegularMarket.End,
            Equity.PostMarket.End,
            Option.EQO.RegularMarket.Start,
            Option.IND.RegularMarket.End,
        ]
    ))
    Detailed = {
        "EQUITY": {
            "prestart": today_at(Equity.PreMarket.Start),
            "regstart": today_at(Equity.RegularMarket.Start),
            "regend": today_at(Equity.RegularMarket.End),
            "postend": today_at(Equity.PostMarket.End),
        },
        "OPTION": {
            "EQO": {
                "regstart": today_at(Option.EQO.RegularMarket.Start),
                "regend": today_at(Option.EQO.RegularMarket.End),
            },
            "IND": {
                "regstart": today_at(Option.IND.RegularMarket.Start),
                "regend": today_at(Option.IND.RegularMarket.End),
            }
        }
    }

    class Explicit:
        class Equity:
            prestart = today_at(Equity.PreMarket.Start)
            regstart = today_at(Equity.RegularMarket.Start)
            regend = today_at(Equity.RegularMarket.End)
            postend = today_at(Equity.PostMarket.End)

        class Option:
            class Equity:
                regstart = today_at(Option.EQO.RegularMarket.Start)
                regend = today_at(Option.EQO.RegularMarket.End)
            EQO = Equity

            class Index:
                regstart = today_at(Option.IND.RegularMarket.Start)
                regend = today_at(Option.IND.RegularMarket.End)
            IND = Index


EQUITY_OPEN = """
{{
    "equity": {{
        "EQ": {{
            "date": "{date}",
            "marketType": "EQUITY",
            "exchange": "NULL",
            "category": "NULL",
            "product": "EQ",
            "productName": "equity",
            "isOpen": true,
            "sessionHours": {{
                "preMarket": [
                    {{
                        "start": "{date}T{prestart}{utc_offset}",
                        "end": "{date}T{regstart}{utc_offset}"
                    }}
                ],
                "regularMarket": [
                    {{
                        "start": "{date}T{regstart}{utc_offset}",
                        "end": "{date}T{regend}{utc_offset}"
                    }}
                ],
                "postMarket": [
                    {{
                        "start": "{date}T{regend}{utc_offset}",
                        "end": "{date}T{postend}{utc_offset}"
                    }}
                ]
            }}
        }}
    }}
}}
""".strip()


OPTION_OPEN = """
{{
    "option": {{

        "EQO": {{
            "date": "{date}",
            "marketType": "OPTION",
            "exchange": "NULL",
            "category": "NULL",
            "product": "EQO",
            "productName": "equity option",
            "isOpen": true,
            "sessionHours": {{
                "regularMarket": [
                    {{
                        "start": "{date}T{regstart}{utc_offset}",
                        "end": "{date}T{regend}{utc_offset}"
                    }}
                ]
            }}
        }},
        "IND": {{
            "date": "{date}",
            "marketType": "OPTION",
            "exchange": "NULL",
            "category": "NULL",
            "product": "IND",
            "productName": "index option",
            "isOpen": true,
            "sessionHours": {{
                "regularMarket": [
                    {{
                        "start": "{date}T{regstart}{utc_offset}",
                        "end": "{date}T{regend_ind}{utc_offset}"
                    }}
                ]
            }}
        }}
    }}
}}
""".strip()


EQUITY_CLOSED = """
{{
    "equity": {{
        "equity": {{
            "date": "{date}",
            "marketType": "EQUITY",
            "exchange": null,
            "category": null,
            "product": "equity",
            "productName": null,
            "isOpen": false,
            "sessionHours": null
        }}
    }}
}}
""".strip()


OPTION_CLOSED = """
{{
    "option": {{
        "option": {{
            "date": "{date}",
            "marketType": "OPTION",
            "exchange": null,
            "category": null,
            "product": "option",
            "productName": null,
            "isOpen": false,
            "sessionHours": null
        }}
    }}
}}
""".strip()
