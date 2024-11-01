from typing import List, TypedDict


class _Account(TypedDict, total=True):
    accountNumber: str
    primaryAccount: bool
    type: str
    nickName: str
    displayAcctId: str
    autoPositionEffect: bool
    accountColor: str


class _StreamerInfo(TypedDict, total=True):
    streamerSocketUrl: str
    schwabClientCustomerId: str
    schwabClientCorrelId: str
    schwabClientChannel: str
    schwabClientFunctionId: str


class _Offers:
    level2Permissions: bool
    mktDataPermission: str


class UserPreferencesResponse(TypedDict, total=True):
    """Schwab user preferences response model

    >>> # Sample response
    >>> import json
    >>>
    >>> from daily.schwab import client_by_idx  # OR: from daily.schwab.client.functions import client_by_idx
    >>>
    >>> client = client_by_idx(1)
    >>> preferences = client.get_preferences()
    >>> print(json.dumps(preferences, indent=4))
    {
        "accounts": [
            {
                "accountNumber": "14065367",
                "primaryAccount": true,
                "type": "BROKERAGE",
                "nickName": "Individual",
                "displayAcctId": "...367",
                "autoPositionEffect": false,
                "accountColor": "Green"
            }
        ],
        "streamerInfo": [
            {
                "streamerSocketUrl": "wss://streamer-api.schwab.com/ws",
                "schwabClientCustomerId": "aca95ce0bdd16b740dc7f88abcf8a889af7e4a8e22a225e41a2c9214b129534c",
                "schwabClientCorrelId": "b02e7f72-ae27-fd7c-d539-181cce2a8ab8",
                "schwabClientChannel": "N9",
                "schwabClientFunctionId": "APIAPP"
            }
        ],
        "offers": [
            {
                "level2Permissions": true,
                "mktDataPermission": "NP"
            }
        ]
    }

    """

    accounts: List[_Account]
    streamerInfo: List[_StreamerInfo]
    offers: List[_Offers]
