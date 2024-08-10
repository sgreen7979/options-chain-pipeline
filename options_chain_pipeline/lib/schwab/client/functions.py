from typing import TYPE_CHECKING

from . import getSchwabClientClass

if TYPE_CHECKING:
    from daily.schwab.credentials import SchwabCredentials


def client_from_credentials(credentials: "SchwabCredentials"):
    # from daily.schwab.client import _schwabClientClass
    # from daily.schwab.client import getSchwabClientClass

    SchwabClient = getSchwabClientClass()

    # return _schwabClientClass.from_credentials(credentials)
    return SchwabClient.from_credentials(credentials)
