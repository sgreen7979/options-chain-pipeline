#!/usr/bin/env python3
from datetime import datetime
from typing import List

import pandas as pd


class CATSymbols:
    URL = "https://files.catnmsplan.com/"
    EQUITIES_ENDPOINT = "symbol-master/FINRACATReportableEquitySecurities_EOD.txt"
    OPTIONS_ENDPOINT = "symbol-master/CATReportableOptionsSymbolMaster_EOD.txt"

    def get_equities(
        self,
        remove_test_symbols: bool = True,
        to_list: bool = False,
        remove_otc_symbols: bool = False,
    ) -> pd.DataFrame | List[str]:
        columns = ["Symbol", "Issue_Name", "Primary_Listing_Mkt", "test_issue_flag"]

        df = pd.read_csv(
            self.build_url(self.EQUITIES_ENDPOINT), sep="|", header=None, names=columns
        )

        # drop row with original column headers
        df.drop(0, axis=0, inplace=True)

        if remove_test_symbols:
            df = df[df.test_issue_flag == 'N'].reset_index(drop=True)
        if remove_otc_symbols:
            df = df[df.Primary_Listing_Mkt != 'U'].reset_index(drop=True)
        return df["Symbol"].unique().tolist() if to_list else df

    def get_options(
        self,
        remove_test_symbols: bool = True,
        to_list: bool = False,
        append_symbol: bool = True,
    ) -> pd.DataFrame | list[str]:
        columns = [
            "optionKind",
            "optionID",
            "underlying",
            "exerciseStyle",
            "settlementType",
            "test_issue_flag",
        ]

        df = pd.read_csv(
            self.build_url(self.OPTIONS_ENDPOINT), sep="|", header=None, names=columns
        )

        # drop row with original column headers
        df.drop(0, axis=0, inplace=True)

        if remove_test_symbols:
            df = df[df.test_issue_flag == 'N'].reset_index(drop=True)

        if to_list:
            symbols_list = sorted(df["underlying"].astype(str).unique().tolist())
            if "nan" in symbols_list:
                symbols_list.remove("nan")
            return symbols_list

        if append_symbol:

            def extract_suffix(optionID: str) -> str:
                suffix_split = optionID.split()
                return (
                    suffix_split[0][-15:]
                    if len(suffix_split) == 1
                    else suffix_split[-1]
                )

            df["_suffix"] = df["optionID"].apply(extract_suffix)

            def format_date(_suffix: str) -> str:
                """Formats expiration date string from YYMMDD to MMDDYY

                Example::

                    >>> date_str = "240119"
                    >>> format_date(date_str)
                    '011924'

                Args:
                    date_str (str): European date string (YYMMDD)

                Returns:
                    str: U.S. formatted date string (MMDDYY)
                """

                if not _suffix:
                    return ""

                date_str = _suffix[:6]

                try:
                    return datetime.strptime(date_str, "%y%m%d").strftime("%m%d%y")
                except ValueError:
                    return ""

            df["_date_str"] = df["_suffix"].apply(format_date)

            def format_strike(_suffix: str) -> str:
                if not _suffix:
                    return ""

                strike_str = _suffix[7:]

                if not strike_str:
                    return ""

                return str(
                    int(strike_str[:-3]) + int(strike_str[-3:]) / 1000
                ).removesuffix(".0")

            df["_strike_str"] = df["_suffix"].apply(format_strike)

            def get_putCall(_suffix: str):
                return "" if not _suffix else _suffix[6]

            df["_putCall"] = df["_suffix"].apply(get_putCall)

            def get_symbol(row: dict):
                if any(
                    [
                        row["_date_str"] == "",
                        row["_strike_str"] == "",
                        row["_putCall"] == "",
                    ]
                ):
                    return ""
                return (
                    row["underlying"]
                    + "_"
                    + row["_date_str"]
                    + row["_putCall"]
                    + row["_strike_str"]
                )

            df["symbol"] = df.apply(get_symbol, axis=1)

        return df

    def build_url(self, endpoint):
        return self.URL + endpoint
