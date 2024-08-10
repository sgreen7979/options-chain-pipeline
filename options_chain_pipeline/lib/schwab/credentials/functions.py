#!/usr/bin/env python3
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING

from daily.utils.logging import get_logger

from ..credentials import CREDENTIALS_DICT
from ..credentials import NUM_ACCOUNTS
from ..exc import InvalidAccountIndexError

if TYPE_CHECKING:
    from .credentials import SchwabCredentials

logger = get_logger(__name__)


def validate_account_idx(acct_idx: int, raise_on_error: bool = False) -> bool:
    """Checks whether account index is valid

    :param acct_idx: non-zero based index
    :type acct_idx: int
    :param raise_on_error: defaults to False
    :type raise_on_error: bool, optional
    :raises InvalidAccountIndexError: if acct_idx is invalid and raise_on_error is True
    :return: whether acct_idx is valid
    :rtype: bool
    """
    result = 0 < acct_idx <= NUM_ACCOUNTS
    if not result and raise_on_error:
        raise InvalidAccountIndexError(
            f"0 < acct_idx < {NUM_ACCOUNTS + 1}, got {acct_idx}"
        )
    return result


def get_credentials(acct_idx: int) -> "SchwabCredentials":
    """
    :param acct_idx: non-zero based index
    """
    validate_account_idx(acct_idx, raise_on_error=True)
    return CREDENTIALS_DICT[acct_idx]


def get_all_credentials() -> List["SchwabCredentials"]:
    return list(map(get_credentials, range(1, NUM_ACCOUNTS + 1)))


def get_api_key_from_acct_idx(acct_idx: int) -> str:
    validate_account_idx(acct_idx, raise_on_error=True)
    return get_credentials(acct_idx).api_key


def get_login(acct_idx: int) -> "SchwabCredentials.Login":
    validate_account_idx(acct_idx, raise_on_error=True)
    return get_credentials(acct_idx).login


def get_login_tuple(acct_idx: int) -> Tuple[Optional[str], Optional[str]]:
    login = get_login(acct_idx)
    if not login:
        logger.warning(
            f"Login data is not populated for {acct_idx}. "
            "To populate this data, set the following "
            "environment variables: "
            f"\n\t'SCHWAB_ACCOUNT_USERNAME_{acct_idx}'"
            f"\n\t'SCHWAB_ACCOUNT_PASSWORD_{acct_idx}'"
        )
    return login.to_tup()


def get_login_dict(acct_idx: int) -> Dict[str, Optional[str]]:
    login = get_login(acct_idx)
    if not login:
        logger.warning(
            f"Login data is not populated for {acct_idx}. "
            "To populate this data, set the following "
            "environment variables: "
            f"\n\tSCHWAB_ACCOUNT_USERNAME_{acct_idx}"
            f"\n\tSCHWAB_ACCOUNT_PASSWORD_{acct_idx}"
        )
    return login.to_dict()
