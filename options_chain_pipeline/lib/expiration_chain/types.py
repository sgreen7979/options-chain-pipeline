from typing import List
from typing import TypedDict


class ExpirationDate(TypedDict):
    expirationDate: str
    daysToExpiration: int
    expirationType: str
    standard: bool


class ExpirationChain(TypedDict):
    expirationList: List[ExpirationDate]
