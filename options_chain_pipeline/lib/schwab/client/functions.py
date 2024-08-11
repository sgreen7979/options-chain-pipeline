#!/usr/bin/env python3
from typing import TYPE_CHECKING

from . import getSchwabClientClass

if TYPE_CHECKING:
    from ..credentials import SchwabCredentials


def client_from_credentials(credentials: "SchwabCredentials"):
    SchwabClient = getSchwabClientClass()
    return SchwabClient.from_credentials(credentials)
