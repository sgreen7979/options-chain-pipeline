#!/usr/bin/env python3
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Tuple


class SchwabCredentials(NamedTuple):
    """
    Attributes
    ----------
    api_key : str
        required
    secret : str
        required
    redirect_uri : str
        required
    token_path : str
        required
    account_id : str
        optional
    user_id : str
        optional, but required for the `login` attribute of this class
    password : str
        optional, but required for the `login` attribute of this class
    idx : int
        coincides with the names of the environment variables set
        e.g., $SCHWAB_API_KEY_1 coincides with the api key of the credentials of index 1
              $SCHWAB_SECRET_3 coincides with the secret of the credentials of index 3
    app_status: str
        if equal to "READY_FOR_USE", it is an active account
    """

    api_key: str
    secret: str
    redirect_uri: str
    token_path: str
    account_number: Optional[str]
    user_id: Optional[str]
    password: Optional[str]
    idx: int
    app_status: str
    developer_email: Optional[str]

    def to_dict(self):
        return {
            "api_key": self.api_key,
            "secret": self.secret,
            "redirect_uri": self.redirect_uri,
            "token_path": self.token_path,
            "account_number": self.account_number,
            "user_id": self.user_id,
            "password": self.password,
            "idx": self.idx,
            "app_status": self.app_status,
            "developer_email": self.developer_email,
        }

    class Login:

        def __init__(self, user_id: Optional[str], password: Optional[str]) -> None:
            self.user_id = user_id
            self.password = password

        def to_dict(self) -> Dict[str, Optional[str]]:
            return {"username": self.user_id, "password": self.password}

        def to_tup(self) -> Tuple[Optional[str], Optional[str]]:
            return (self.user_id, self.password)

        __tuple__ = to_tup

        @property
        def is_null(self) -> bool:
            return not self.populated

        @property
        def populated(self) -> bool:
            return (self.user_id is not None) and (self.password is not None)

        __bool__ = populated

    @property
    def login(self) -> Login:
        return self.Login(self.user_id, self.password)

    @property
    def login_tuple(self) -> Tuple[Optional[str], Optional[str]]:
        # return (self.user_id, self.password)
        return self.login.to_tup()

    @property
    def login_dict(self) -> Dict[str, Optional[str]]:
        # return {"username": self.user_id, "password": self.password}
        return self.login.to_dict()

    def index(self) -> int:
        return self.idx
