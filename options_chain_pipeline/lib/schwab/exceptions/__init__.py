import json
import requests
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt

from daily.utils.requests_dataclasses import _extract_params_from_url


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
