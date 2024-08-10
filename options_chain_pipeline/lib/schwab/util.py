#!/usr/bin/env python3

import datetime
import os


def get_number_of_accounts(raise_on_failure: bool = False):
    # num_accounts = len(search_for_api_keys())
    num_accounts = search_for_active_accounts()
    if not num_accounts and raise_on_failure:
        from daily.schwab.exc import NoCredentialsFoundException

        raise NoCredentialsFoundException(
            "Failed to retrieve Schwab account information from environment."
        )
    return num_accounts


def search_for_api_keys():
    """List of keys in os.environ whose values are Schwab API Keys."""

    def check(key: str) -> bool:
        return "SCHWAB_API_KEY" in key

    return list(filter(check, list(os.environ.keys())))


def search_for_active_accounts() -> int:
    """Returns the number of active accounts

    Active accounts are those whose corresponding environment
    variable for app status ("SCHWAB_APP_STATUS_{idx}") equals
    "READY_FOR_USE"

    """

    def get_idx(k: str) -> str:
        return k[-1]

    idxs = list(map(get_idx, search_for_api_keys()))

    def check(idx: str) -> bool:
        return os.getenv(f"SCHWAB_APP_STATUS_{idx}") == "READY_FOR_USE"

    return sum(map(check, idxs))


def chromedriver():
    """Use to create new token when getting wonky login issues,
    like bad code 400 or Login denied.

    When creating new tokens, delete appropriate token file
    MAKE SURE to login with the appropriate login credentials

    """
    import atexit
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    driver = webdriver.Chrome(ChromeDriverManager().install())
    atexit.register(lambda: driver.quit())
    return driver
