import json
from typing import Optional
from typing import TYPE_CHECKING
import urllib.parse

import requests

if TYPE_CHECKING:
    import datetime as dt


def _extract_params_from_url(url: Optional[str]):
    if url is None:
        return {}

    def fmt(value: str):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            return value

    return {
        # k: fmt(v[0])
        k: v[0]
        for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(url).query).items()
    }


class SchwabRequestError(Exception):
    def __init__(self, message, fetch_time, response=None, request=None) -> None:
        self._message = message
        self.fetch_time = fetch_time
        self.response = response
        if response is None and self.request is None:
            self.request = None
        elif isinstance(response, requests.Response):
            self.request = response.request
        elif isinstance(request, requests.PreparedRequest):
            self.request = request
        super().__init__(str(self))

    def __str__(self):
        message = self._message
        if self.request is not None:
            message = message[:-1]
            endpoint = self.request.path_url.split("?")[0]
            params = _extract_params_from_url(self.request.url)
            return (
                f"{message},\"params\":{json.dumps(params)},\"endpoint\":{json.dumps(endpoint)}"
                + "}"
            )
        else:
            return message


# Adapted from https://github.com/areed1192/td-ameritrade-python-api by Alex Reed
# todo: reconcile any Schwab changes
class TknExpError(Exception):
    """Raise exception when refresh or access token is expired.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class ExdLmtError(Exception):
    """Raise exception when exceeding query limit of the server.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class NotNulError(Exception):
    """Raise exception when a null value is passed into non-null field.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class ForbidError(Exception):
    """Raise forbidden exception. This usually occurs when the app does
    not have access to the account.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class NotFndError(Exception):
    """Raise exception when criteria is not found.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class ServerError(Exception):
    """Raise exception when there is an error with the service or the server
    cannot provide response.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class GeneralError(Exception):
    """Raise exception for all other status code >400 errors which are not
    defined above.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(self, message):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        super().__init__(self.message)


class UnexpectedTokenAuthError(Exception):
    """Raise exception for all other status code >400 errors which are not
    defined above.

    ### Arguments:
    ----
    Exception (Exception): The base python exception class
    """

    def __init__(
        self, message, fetch_time: "dt.datetime", response: "requests.Response"
    ):
        """Print out message for this exception.

        Arguments:
        ----
        message (str): Pass in the message returned by the server.
        """
        self.message = message
        self.fetch_time = fetch_time
        self.response = response
        super().__init__(self.message)


class ReadTimeoutError(requests.exceptions.ReadTimeout):
    def __init__(
        self,
        message,
        fetch_time: "dt.datetime",
        prepared_request: "requests.PreparedRequest",
    ):
        self.message = message
        self.fetch_time = fetch_time
        self.prepared_request = prepared_request
        super().__init__(self.message)


class InvalidResponseDataError(Exception):
    """
    Raised when data returned by option chain get request is invalid
    """
    
    def __init__(self, sym: str, status_code: int):
        self.sym = sym
        self.status_code = status_code
        super().__init__(self.__str__())

    def __str__(self):
        cls_name = type(self).__name__
        return f"{cls_name}(underlying={self.sym}, status={self.status_code})"


class ValidationException(Exception):
    __alias__ = "val"

    def __init__(self, msg, val):
        self.msg = msg
        self.val = val
        super().__init__(self.__str__())

    def __str__(self):
        return f"{self.msg} ({self.__alias__}={self.val})"


class InvalidApiKeyException(ValidationException):
    __alias__ = "apikey"


class InvalidAcctIdException(ValidationException):
    __alias__ = "acctid"


class InvalidAccountIndexError(Exception):
    """Raised when an invalid account number is passed"""


class EmptyLoginCredentialsException(Exception):
    """Raised when either User ID or Password credentials is `None`."""


class NoCredentialsFoundException(Exception):
    """Raised when no account information can be found within
    environment variables."""
