# Adapted from https://github.com/areed1192/td-ameritrade-python-api by Alex Reed
import asyncio
import datetime as dt
import json
import os
import pathlib
import time
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)
import urllib.parse

import requests

from daily.utils.logging import (
    get_logger,
    ClassNameLoggerMixin,
    LoggerAdapter,
    Formatter,
)
from daily.utils.requests_dataclasses import _extract_params_from_url

from ..credentials.functions import get_credentials
from ..enums import VALID_CHART_VALUES
from .. import exceptions as exc
from ..expirations import ExpirationChainResponse
from ..handlers.base import ResponseHandler
from ..option_chain.models import OptionsChainResponse
from ..market_hours import (
    MarketHoursResponse,
    ############################
    BondOnlyResponse,
    OptionOnlyResponse,
    EquityOnlyResponse,
    FutureOnlyResponse,
    ############################
    BondEquityResponse,
    BondFutureResponse,
    BondOptionResponse,
    EquityFutureResponse,
    OptionFutureResponse,
    OptionEquityResponse,
    ############################
    BondEquityFutureResponse,
    BondOptionEquityResponse,
    BondOptionFutureResponse,
    OptionEquityFutureResponse,
    ############################
    AllMarketsResponse,
)
from ..option_chain import OptionsChainParams
from ..orders import Order
from ..preferences import UserPreferencesResponse
from ..session.session_proto import SyncSessionProto
from .types import ConfigDict

if TYPE_CHECKING:
    from ..credentials import SchwabCredentials

LOG_LEVEL = "DEBUG"
logger = get_logger(__name__, LOG_LEVEL)

_T = TypeVar("_T")
_H_co = TypeVar("_H_co", bound=ResponseHandler, covariant=True)
_SessionProto = SyncSessionProto[requests.Response]
# _ModeT = TypeVar("_ModeT", bound=Literal["form", "json"])
_ModeT = Literal["form", "json"]


class SessionFactoryT(Protocol):
    def __call__(*args: Any, **kwargs: Any) -> _SessionProto: ...


class BaseSchwabClient(ClassNameLoggerMixin):
    """Schwab API Client Class.

    Implements OAuth 2.0 Authorization Co-de Grant workflow, handles configuration
    and state management, adds token for authenticated calls, and performs request
    to the Schwab API.

    """

    LEVEL = "INFO"
    CH = True
    FORMATTER = Formatter(
        "%(asctime)s %(levelname)s %(name)s<%(idx)d> %(message)s", defaults={"idx": 0}
    )
    PROPAGATE = False
    # FH_TYPE = "RotatingFileHandler"
    # FH_TYPE_KWARGS = {"maxBytes": 1_048_576, "backupCount": 500_000}

    _DEFAULT_SESSION_FACTORY: ClassVar[SessionFactoryT] = requests.Session
    config: ClassVar[ConfigDict] = {
        "api_endpoint": "https://api.schwabapi.com",
        "auth_endpoint": "https://api.schwabapi.com/v1/oauth/authorize?",
        "token_endpoint": "https://api.schwabapi.com/v1/oauth/token",
        "refresh_enabled": True,
        "refresh_token_expires_in": 604800,  # 7 days
        "rolling_sixty_limit": 120,
    }

    @overload
    def __init__(
        self,
        idx: Optional[int] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        credentials_path: Optional[str] = None,
        account_number: Optional[str] = None,
        auth_flow: str = "default",
        _do_init: bool = True,
        *,
        credentials: "SchwabCredentials",
        response_handler: Optional[ResponseHandler] = None,
        session_factory: Optional[SessionFactoryT] = None,
        token_writer=None,
        token_loader=None,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        idx: int,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        credentials_path: Optional[str] = None,
        account_number: Optional[str] = None,
        auth_flow: str = "default",
        _do_init: bool = True,
        *,
        credentials: Optional["SchwabCredentials"] = None,
        response_handler: Optional[ResponseHandler] = None,
        session_factory: Optional[SessionFactoryT] = None,
        token_writer=None,
        token_loader=None,
        **kwargs,
    ) -> None: ...

    def __init__(
        self,
        idx: Optional[int] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        credentials_path: Optional[str] = None,
        account_number: Optional[str] = None,
        auth_flow: str = "default",
        _do_init: bool = True,
        *,
        credentials: Optional["SchwabCredentials"] = None,
        response_handler: Optional[ResponseHandler] = None,
        session_factory: Optional[SessionFactoryT] = None,
        token_writer=None,
        token_loader=None,
        **kwargs,
    ) -> None:
        """
        ### Usage:
        ----
            >>> # Credentials Path & Account Specified.
            >>> schwab_client = Schwab(
                client_id= '<CLIENT_ID>', (the "APP KEY" from Schwab)
                client_secret= '<CLIENT_SECRET>'
                redirect_uri='<REDIRECT_URI>',
                account_number='<ACCOUNT_NUMBER>',
                credentials_path='<CREDENTIALS_PATH>'
            )
            >>> schwab_client.login()
        """
        if credentials is not None:
            self._credentials = credentials
        elif idx is not None:
            self._credentials = get_credentials(idx)
        else:
            raise ValueError("credentials or idx must be set")

        self.account_number: Optional[str] = self._credentials.account_number
        self.base64_client_id: str = self._credentials._base64_client_id()
        self.client_id: str = self._credentials.api_key
        self.client_secret: str = self._credentials.secret
        self.credentials_path: pathlib.Path = pathlib.Path(self._credentials.token_path)
        self.idx: int = self._credentials.idx
        self.redirect_uri: str = self._credentials.redirect_uri

        # Define the initalized state, these are the default values.
        self.state: dict = {"access_token": None, "refresh_token": None}
        self.auth_flow: str = auth_flow

        # Define a new attribute called 'authstate' and initialize to `False`. This will be used by our login function.
        self.authstate: bool = False

        self._token_loader = token_loader
        self._token_writer = token_writer

        # call the state_manager method and update the state to init (initalized)
        if _do_init:
            self._state_manager("init")

        # if (sessionFactory := kwargs.pop("sessionFactory", None)) is not None:
        self._sessionFactory = session_factory or type(self)._DEFAULT_SESSION_FACTORY

        if type(self).__name__.lower().startswith("async"):
            self.request_session = None
        else:
            self.request_session = self._sessionFactory()
            self.request_session = None

        # if session_factory is None:
        #     self._sessionFactory = self.__class__._DEFAULT_SESSION_FACTORY
        #     self.request_session = self._sessionFactory()
        #     # self.request_session.verify = True
        # else:
        #     self._sessionFactory = session_factory
        #     self.request_session = self._sessionFactory()

        self._response_handler: ResponseHandler = response_handler or ResponseHandler(
            self
        )

    def get_logger_adapter(self):
        return LoggerAdapter(self.__class__.get_logger(), extra={"idx": self.idx})

    @property
    def response_handler(self):
        return self._response_handler

    def set_response_handler(self, rh) -> None:
        self._response_handler = rh

    def _state_manager(self, action: Literal["init", "save"]) -> None:
        """Manages the session state.

        Manages the self.state dictionary. Initalize State will set
        the properties to their default value. Save will save the
        current state to file.

        ### Arguments:
        ----
        action {str}: action argument must of one of the following:
            'init' -- Initalize State.
            'save' -- Save the current state.
        """

        if self.credentials_path.exists() and not self.credentials_path.read_text():
            self.credentials_path.unlink()

        # If the file exists then load it.
        if action == "init" and self.credentials_path.exists():

            with open(file=self.credentials_path, mode="r") as json_file:
                if self._token_loader is not None:
                    self.state.update(self._token_loader.load(json_file))
                else:
                    self.state.update(json.load(json_file))

        # Saves the credentials file.
        elif action == "save":
            with open(file=self.credentials_path, mode="w+") as json_file:
                if self._token_writer is not None:
                    self._token_writer.write(self.state, json_file)
                else:
                    json.dump(obj=self.state, fp=json_file, indent=4)

    def login(self) -> bool:
        """Logs the user into the Broker API.

        Ask the user to authenticate  themselves via the Schwab Authentication Portal. This will
        create a URL, display it for the User to go to and request that they paste the final URL into
        command window. Once the user is authenticated the API key is valide for 90 days, so refresh
        tokens may be used from this point, up to the 90 days.

        ### Returns:
        ----
        {bool} -- Specifies whether it was successful or not.
        """

        # Only attempt silent SSO if the credential file exists.
        # only at login
        exists = self.credentials_path.exists()

        if (
            exists and self._silent_sso()
        ):  # schwab_credentials exists and validate_tokens is True
            self.authstate = True
            # return True
        else:
            # no credentials file or the refresh_token expired -> triggers the oauth flow
            self.authstate = self.oauth()
            # return self.authstate
        return self.authstate

    def logout(self) -> None:
        """Clears the current Broker Connection state."""
        # change state to initalized so they will have to either get a
        # new access token or refresh token next time they use the API
        self._state_manager("init")

    def __del__(self):
        # clear session
        # if hasattr(self, "request_session") and self.request_session is not None:
        #     self.request_session.close()
        pass

    """
    -----------------------------------------------------------
    THIS BEGINS THE AUTH SECTION.
    -----------------------------------------------------------
    """

    def _requires_oauth(self) -> bool:
        if (
            "refresh_token_expires_at" in self.state
            and "access_token_expires_at" in self.state
        ):
            # should be true if _token_save already ran as part of oauth flow

            # Grab the refresh_token expire Time.
            refresh_token_exp = self.state["refresh_token_expires_at"]
            assert isinstance(refresh_token_exp, float)
            refresh_token_ts = dt.datetime.fromtimestamp(refresh_token_exp)
            # Grab the Expire Threshold
            refresh_token_exp_threshold = refresh_token_ts - dt.timedelta(days=1)
            # Convert Thresholds to Seconds.
            refresh_token_exp_threshold = refresh_token_exp_threshold.timestamp()
            # Check the Refresh Token first, is expired or expiring soon?
            self.get_logger_adapter().debug(
                f"The refresh token will expire at: {self.state['refresh_token_expires_at_date']}"
            )
            if dt.datetime.now().timestamp() > refresh_token_exp_threshold:
                return True
            # Grab the access_token expire Time.
            access_token_exp = self.state["access_token_expires_at"]
            assert isinstance(access_token_exp, float)
            access_token_ts = dt.datetime.fromtimestamp(access_token_exp)
            # Grab the Expire Thresholds.
            access_token_exp_threshold = access_token_ts - dt.timedelta(minutes=5)
            # Convert Thresholds to Seconds.
            access_token_exp_threshold = access_token_exp_threshold.timestamp()
            # See if we need a new Access Token.
            if dt.datetime.now().timestamp() > access_token_exp_threshold:
                return True

        return False

    def get_account_number_hash_values(self) -> dict:
        endpoint = "trader/v1/accounts/accountNumbers"
        # make the request.
        return self._make_request(method="get", endpoint=endpoint)

    def grab_access_token(self) -> Optional[dict]:
        # called by validate_tokens (the access_token is expired)
        """Refreshes the current access token.

        This takes a valid refresh token and refreshes
        an expired access token. This is different from
        exchanging a code for an access token.

        Returns:
        ----
        dict or None: The token dictionary if successful, None otherwise.
        """
        try:
            # Build the parameters of the request
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.state["refresh_token"],
            }
            # Make the request
            response = requests.post(
                url=self.config["token_endpoint"],
                headers=self._create_request_headers(is_access_token_request=True),
                data=data,
            )
            if response.ok:
                self.get_logger_adapter().info(
                    f"Grab acces token resp: {response.json()}"
                )
                return self._token_save(
                    token_dict=response.json(), refresh_token_from_oauth=False
                )
            else:
                # Log the error
                self.get_logger_adapter().error(
                    f"Failed to refresh access token. Status code: {response.status_code}, Reason: {response.text}"
                )
                if "expired" in response.text:
                    self.get_logger_adapter().info(
                        "oauth called from grab_access_token"
                    )
                    self.oauth()
        except requests.RequestException as e:
            # Log the exception
            self.get_logger_adapter().error(f"Failed to refresh access token: {e}")
            raise

    def oauth(self) -> bool:
        """Runs the oAuth process for the Broker API."""
        # called by login(no credentials file) and validate_tokens (a token is expired) or _make_request if response not OK
        # Create the Auth URL.
        url = f"{self.config['auth_endpoint']}client_id={self.client_id}&redirect_uri={self.redirect_uri}"

        print(
            f"Please go to URL provided to authorize your account(idx={self.idx}): {url}"  # type: ignore
        )
        # Paste it back and send it to exchange_code_for_token.
        redirect_url = input("Paste the full URL redirect here: ")

        self.get_logger_adapter().debug(f"redirect_url: {redirect_url}")
        self.code = self._extract_redirect_code(redirect_url)
        self.get_logger_adapter().debug(f"self.code: {self.code}")

        # Exchange the Auth Code for an Access Token.
        return self.exchange_code_for_token()

    def exchange_code_for_token(self):
        # called by oauth
        """Access token handler for AuthCode Workflow.
        This takes the authorization code parsed from
        the auth endpoint to call the token endpoint
        and obtain an access token.

        ### Returns: {bool} -- `True` if successful, `False` otherwise.
        """
        url_code = self.code  # ?
        # Define the parameters of our access token post.
        data = {
            "grant_type": "authorization_code",
            "code": url_code,
            "redirect_uri": self.redirect_uri,
        }
        self.get_logger_adapter().info(f"data = {data}")
        # Make the request.
        response = requests.post(
            url=self.config["token_endpoint"],
            headers=self._create_request_headers(
                mode="form", is_access_token_request=True
            ),
            data=data,
        )

        if response.ok:
            self.get_logger_adapter().info(
                f"Exchange code for token resp: {response.json()}"
            )
            self._token_save(token_dict=response.json(), refresh_token_from_oauth=True)
            return True
        else:
            # Handle the case where the request fails
            self.get_logger_adapter().info(
                f"Exchange_code_for_token request failed {response.status_code}, {response.text}"
            )
            return False

    def validate_tokens(self) -> bool:
        # called by _silent_sso at first login and  each _make_request
        # this function is only checking for token expiration times, nothing else
        """
        ### Returns
        -------
        bool
            Returns `True` if the tokens are not expired, `False` if
            they are.
        """
        if (
            "refresh_token_expires_at" in self.state
            and "access_token_expires_at" in self.state
        ):
            # should be true if _token_save already ran as part of oauth flow

            # Grab the refresh_token expire Time.
            refresh_token_exp = self.state["refresh_token_expires_at"]
            assert isinstance(refresh_token_exp, float)
            refresh_token_ts = dt.datetime.fromtimestamp(refresh_token_exp)
            # Grab the Expire Threshold
            refresh_token_exp_threshold = refresh_token_ts - dt.timedelta(days=1)
            # Convert Thresholds to Seconds.
            refresh_token_exp_threshold = refresh_token_exp_threshold.timestamp()
            # Check the Refresh Token first, is expired or expiring soon?
            self.get_logger_adapter().debug(
                f"The refresh token will expire at: {self.state['refresh_token_expires_at_date']}"
            )
            if dt.datetime.now().timestamp() > refresh_token_exp_threshold:
                self.oauth()
            # Grab the access_token expire Time.
            access_token_exp = self.state["access_token_expires_at"]
            assert isinstance(access_token_exp, float)
            access_token_ts = dt.datetime.fromtimestamp(access_token_exp)
            # Grab the Expire Thresholds.
            access_token_exp_threshold = access_token_ts - dt.timedelta(minutes=5)
            # Convert Thresholds to Seconds.
            access_token_exp_threshold = access_token_exp_threshold.timestamp()
            # See if we need a new Access Token.
            if dt.datetime.now().timestamp() > access_token_exp_threshold:
                self.get_logger_adapter().debug("Grabbing new access token...")
                # print("Grabbing new access token...")
                self.grab_access_token()
            return True
        else:
            # token expire times are not in self.state
            return self.oauth()

    def _silent_sso(self) -> bool:
        # called by login
        # just returns bool from validate_tokens, just checks expirations
        """
        Overview:
        ----
        Attempt a silent authentication, by checking whether current
        access token has expired yet and/or attempting to refresh it. Returns
        True if we have successfully stored a valid access token.

        ### Returns:
        ----
        {bool} -- Specifies whether it was successful or not.
        """
        return self.validate_tokens()

    def _token_save(
        self, token_dict: dict, refresh_token_from_oauth: bool = False
    ) -> dict:  # called by grab_access_token and exchange_code_for_token
        """Parses the token and saves it.
        Overview:
        ----
        Parses an access token from the response of a POST request and saves it
        in the state dictionary for future use. Additionally, it will store the
        expiration time and the refresh token.

        Arguments:
        ----
        token_dict {dict} -- A response object received from the `exchange_code_for_token` or
            `grab_access_token` methods.

        Returns:
        ----
        {dict} -- A token dictionary with the new added values.
        """

        # Calculate access_token expiration times
        access_token_expire = time.time() + int(token_dict["expires_in"])
        access_token_expires_at_date = dt.datetime.fromtimestamp(
            access_token_expire
        ).isoformat()

        # Prepare data to update the state, Save everything returned even if we're not using it.
        # saves refresh_token everytime whether it's new or not
        state_updates = {
            "expires_in": token_dict["expires_in"],
            "token_type": token_dict["token_type"],
            "scope": token_dict["scope"],
            "access_token": token_dict["access_token"],
            "access_token_expires_at": access_token_expire,
            "access_token_expires_at_date": access_token_expires_at_date,
            "id_token": token_dict["id_token"],
            "refresh_token": token_dict["refresh_token"],
        }

        # Update refresh_token & expiration data if necessary (we came here from oauth)
        if refresh_token_from_oauth:
            refresh_token_expire = time.time() + int(
                self.config["refresh_token_expires_in"]
            )
            state_updates.update(
                {
                    "refresh_token": token_dict["refresh_token"],
                    "refresh_token_expires_at": refresh_token_expire,
                    "refresh_token_expires_at_date": dt.datetime.fromtimestamp(
                        refresh_token_expire
                    ).isoformat(),
                }
            )

        # Update state
        self.state.update(state_updates)

        # Save state
        self._state_manager("save")

        return self.state

    """
    -----------------------------------------------------------
    THIS BEGINS THE ACCOUNTS ENDPOINTS PORTION.
    -----------------------------------------------------------
    """

    def _api_endpoint(self, endpoint: str) -> str:
        # called by _make_request
        """Convert relative endpoint to full API endpoint."""
        return f"{self.config['api_endpoint']}/{endpoint}"

    def _create_request_headers(
        self, mode: Optional[_ModeT] = None, is_access_token_request: bool = False
    ) -> dict:  # called by _make_request, grab_access_token, exchange_code_for_token
        """Create the headers for a request.

        Returns a dictionary of default HTTP headers for calls to the broker API,
        in the headers we defined the Authorization and access token.

        ### Arguments:
        ----
        is_access_token_request {bool}. Are the headers for an oauth request? default:False
        mode {str} -- Defines the content-type for the headers dictionary. (default: {None})

        ### Returns:
        ----
        {dict} -- Dictionary with the Access token and content-type
            if specified
        """
        if is_access_token_request:
            return {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {self.base64_client_id}",
            }

        else:
            headers = {"Authorization": f"Bearer {self.state['access_token'] }"}

            if mode == "json":
                headers["Content-Type"] = "application/json"
            elif mode == "form":
                headers["Content-Type"] = "application/x-www-form-urlencoded"

        return headers

    def _prepare_request(
        self,
        method: str,
        endpoint: str,
        mode: Optional[_ModeT] = None,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> "requests.PreparedRequest":
        url = self._api_endpoint(endpoint=endpoint)
        headers = self._create_request_headers(mode=mode)

        # logging.info(f"Request URL: {url}")
        # logging.info(f"the headers: {headers}")
        # logging.info(f"the params: {params}")

        return requests.Request(
            method=method.upper(),
            headers=headers,
            url=url,
            params=params,
            data=data,
            json=json,
        ).prepare()

    def _get_response(self, request: "requests.PreparedRequest", **kwargs):
        request_session = self.request_session or self._sessionFactory()
        # kwargs.setdefault("stream", request_session.stream)
        # kwargs.setdefault("cert", request_session.cert)
        return request_session.get(
            url=str(request.url),
            headers=request.headers,
            verify=True,
            **kwargs,
        )

    def _handle_response(self, *args, **kwargs):
        return self.response_handler.handle(*args, **kwargs)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        **kwargs,
        # mode: Optional[str] = None,
        # data: Optional[dict] = None,
        # json: Optional[dict] = None,
        # order_details: bool = False,
        # incl_fetch_time: bool = False,
        # multi: Optional[int] = None,
        # incl_response: bool = False,
        # **kwargs,
    ) -> dict:
        # ) -> tuple["requests.Response", "requests.PreparedRequest", dict]:
        # Make sure the token is valid if it's not a Token API call.
        self.validate_tokens()

        prepared_request = self._prepare_request(
            method=method,
            endpoint=endpoint,
            # mode=mode,
            mode=kwargs.get("mode", None),
            # params=params,
            params=params,
            data=kwargs.get("data", None),
            json=kwargs.get("json", None),
        )

        response = self._get_response(prepared_request, timeout=15)

        return self.response_handler.handle(
            response,
            prepared_request,
            kwargs,
        )

    @overload
    def _prepare_arguments_list(self, parameter_list: List[_T]) -> List[_T]: ...

    @overload
    def _prepare_arguments_list(self, parameter_list: Sequence[_T]) -> Sequence[_T]: ...

    def _prepare_arguments_list(
        self, parameter_list: Union[List[_T], Sequence[_T]]
    ) -> Union[List[_T], Sequence[_T]]:
        """
        Prepares a list of parameter values for certain API calls.

        Some endpoints accept multiple values for a parameter. This method takes a list
        of parameter values and creates a comma-separated string that can be used in an API request.

        Arguments:
        ----
        parameter_list: A list of parameter values assigned to an argument.

        Usage:
        ----
            >>> schwab_client._prepare_arguments_list(['MSFT', 'SQ'])
        """

        # Ensure parameter_list is not None and is a list
        if parameter_list is None:
            raise ValueError("Parameter list cannot be None")
        if not isinstance(parameter_list, list):
            raise TypeError("Parameter list must be a list")

        return parameter_list

    def _prepare_quotes_query(
        self,
        instruments: List[str],
        fields: Optional[List[str]] = None,
        indicative: bool = False,
    ) -> Dict:
        params = {}
        instruments = self._prepare_arguments_list(parameter_list=instruments)
        # _prepared_query["instruments"] = instruments

        # Prepare fields list if provided.
        if fields:
            fields = self._prepare_arguments_list(parameter_list=fields)

        # Build the params dictionary.
        params = {"symbols": instruments}
        if fields:
            params["fields"] = fields
        params["indicative"] = indicative  # type: ignore

        # Define the endpoint.
        endpoint = "marketdata/v1/quotes"

        return {
            "endpoint": endpoint,
            "params": params,
        }

    def get_quotes(
        self,
        instruments: List[str],
        fields: Optional[List[str]] = None,
        indicative: bool = False,
    ) -> Dict:
        """
        Get quotes for specified instruments.
        Works for a single instrument or multiple
        Arguments:
        ----
        instruments {List[str]} -- List of instrument symbols.

        fields {Optional[List[str]]} -- List of fields to include in the quotes (default: None).
        Request for subset of data by passing coma separated list of root nodes,
        possible root nodes are:
        quote, fundamental, extended, reference, regular.
        Sending quote, fundamental in request will return quote and fundamental data in response.
        Dont send this attribute for full response.

        indicative:bool=False -- Include indicative symbol quotes for all ETF symbols in request.
        If ETF symbol ABC is in request and indicative=true API will return
        quotes for ABC and its corresponding indicative quote for $ABC.IV
        Available values : true, false


        Returns:
        ----
        {Dict} -- Dictionary containing the quotes data.
        """
        # Prepare instruments list for the request.
        instruments = self._prepare_arguments_list(parameter_list=instruments)

        # Prepare fields list if provided.
        if fields:
            fields = self._prepare_arguments_list(parameter_list=fields)

        # Build the params dictionary.
        params = {"symbols": instruments}
        if fields:
            params["fields"] = fields
        params["indicative"] = indicative  # type: ignore

        # Define the endpoint.
        endpoint = "marketdata/v1/quotes"

        # Return the response of the get request.
        return self._make_request(
            method="get", endpoint=endpoint, params=params, multi=len(instruments)
        )

    def get_orders_path(
        self,
        account: Optional[str] = None,
        max_results: Optional[int] = None,
        from_entered_time: Optional[str] = None,
        to_entered_time: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict:
        """
        Returns all the orders for a specific account, or all orders for all linked accounts if no account is specified.

        Arguments:
        ----
        account: The account number that you want to query for orders. Leave out for all linked accounts

        Keyword Arguments:
        ----
        max_results: The maximum number of orders to retrieve.
        from_entered_time: Specifies the start time to query for orders. Default is 10 days ago.
        to_entered_time: Specifies the end time to query for orders. Default is current time.
        status: Specifies the status of orders to be returned.

        Usage:
        ----
            >>> schwab_client.get_orders_path(
                account='MyAccountID',
                max_results=6,
                from_entered_time='yyyy-MM-ddTHH:mm:ss.SSSZ',
                to_entered_time='yyyy-MM-ddTHH:mm:ss.SSSZ',
                status='FILLED'
            )
        """
        # Set default values for from_entered_time and to_entered_time if not provided
        # Mimics TDA legacy API which did not 'require' these parameters
        if not to_entered_time:
            to_entered_time = self._utcformat(dt.datetime.now())

        if not from_entered_time:
            from_entered_time = self._utcformat(
                dt.datetime.now() - dt.timedelta(days=60)
            )

        # Define the payload
        params = {
            "maxResults": max_results,
            "fromEnteredTime": from_entered_time,
            "toEnteredTime": to_entered_time,
            "status": status,
        }

        # Define the endpoint

        endpoint = (
            "trader/v1/orders"
            if not account
            else f"trader/v1/accounts/{account}/orders"
        )  # All linked accounts or specific account

        # Make the request
        return self._make_request(method="get", endpoint=endpoint, params=params)

    def get_order(self, account: str, order_id: str) -> Dict:
        """
        Returns a specific order for a specific account.

        Arguments:
        ----
        account {str} -- The account number that you want to query orders for.

        Keyword Arguments:
        ----
        order_id {str} -- The ID of the order you want to retrieve.

        Usage:
        ----
            >>> schwab_client.get_orders(account='MyAccountID', order_id='MyOrderID')

        Returns:
        ----
        {Dict} -- A response dictionary.
        """

        # Define the endpoint
        endpoint = f"trader/v1/accounts/{account}/orders/{order_id}"
        # Make the request
        return self._make_request(method="get", endpoint=endpoint)

    def get_accounts(
        self, account: str | None = None, fields: str | None = None
    ) -> Dict:
        """
        Queries accounts for a user.

        Serves as the mechanism to make a request to the "Get Accounts" and "Get Account" Endpoint.
        If one account is provided, a "Get Account" request will be made. If more than one account
        is provided, then a "Get Accounts" request will be made.

        Arguments:
        ----
        account {Optional[str]} -- The account number you wish to receive data on.
                                    Default value is None, which will return all accounts of the user.

        fields {Optional[str]} -- Schwab accepts only "positions" here.

        Usage:
        ----
                >>> schwab_client.get_accounts(
                    account='MyAccountNumber',
                    fields='positions'
                )
        """
        # Schwab accepts only "positions" here.
        # Build the params dictionary.
        params = {"fields": fields} if fields else None

        # Determine the endpoint based on the provided account.
        endpoint = (
            "trader/v1/accounts" if not account else f"trader/v1/accounts/{account}"
        )

        # Return the response of the get request.
        return self._make_request(method="get", endpoint=endpoint, params=params)

    def cancel_order(self, account: str, order_id: str) -> Dict:
        """
        Cancel a specific order for a specific account.

        Arguments:
        ----
        account {str} -- The account number for which the order was made.
        order_id {str} -- The ID of the order to be cancelled.

        Usage:
        ----
            >>> schwab_client.cancel_order(account='MyAccountID', order_id='MyOrderID')

        Returns:
        ----
        {Dict} -- A response dictionary.
        """

        # define the endpoint
        endpoint = f"trader/v1/accounts/{account}/orders/{order_id}"

        return self._make_request(
            method="delete", endpoint=endpoint, order_details=True
        )

    def place_order(self, account: str, order: Dict) -> Dict:
        """
        Places an order for a specific account.

        Arguments:
        ----
        account {str} -- The account number for which the order should be placed.

        order {Dict} -- The order payload.

        Usage:
        ----
            >>> schwab_client.place_order(account='MyAccountID', order={'orderKey': 'OrderValue'})

        Returns:
        ----
        {Dict} -- A response dictionary.
        """

        # Check if the order is an instance of Order class, and extract order details if so
        if isinstance(order, Order):
            order = order._grab_order()

        return self._make_request(
            method="post",
            endpoint=f"trader/v1/accounts/{account}/orders",
            mode="json",
            json=order,
            order_details=True,
        )

    def modify_order(self, account: str, order: Dict, order_id: str) -> Dict:
        """
        Modifies an existing order.
        Arguments:
        ----
        account {str} -- The account number for which the order was placed.

        order {Dict} -- The new order payload.

        order_id {str} -- The ID of the existing order.

        Usage:
        ----
            >>> schwab_client.modify_order(account='MyAccountID', order={'orderKey': 'OrderValue'}, order_id='MyOrderID')

        Returns:
        ----
        {Dict} -- A response dictionary.
        """

        # Check if the order is an instance of Order class, and extract order details if so
        if isinstance(order, Order):
            order = order._grab_order()

        # Make the request
        endpoint = f"trader/v1/accounts/{account}/orders/{order_id}"

        return self._make_request(
            method="put",
            endpoint=endpoint,
            mode="json",
            json=order,
            order_details=True,
        )

    def get_transactions(
        self,
        account: str,
        transaction_type: Optional[
            Literal[
                "TRADE",
                "RECEIVE_AND_DELIVER",
                "DIVIDEND_OR_INTEREST",
                "ACH_RECEIPT",
                "ACH_DISBURSEMENT",
                "CASH_RECEIPT",
                "CASH_DISBURSEMENT",
                "ELECTRONIC_FUND",
                "WIRE_OUT",
                "WIRE_IN",
                "JOURNAL",
                "MEMORANDUM",
                "MARGIN_CALL",
                "MONEY_MARKET",
                "SMA_ADJUSTMENT",
            ]
        ] = None,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        transaction_id: Optional[str] = None,
    ) -> Dict:
        if not start_date:
            start_date = self._utcformat(dt.datetime.now() - dt.timedelta(days=60))
        else:
            pass  # make sure it's within 60 days from now
        if not end_date:
            end_date = self._utcformat(dt.datetime.now())

        # default to a "Get Transaction" Request if anything else is passed through along with the transaction_id.
        if transaction_id is not None:
            account = account
            transaction_type_tuple = (None,)
            start_date_tuple = (None,)
            end_date = None

        # if the request type they made isn't valid print an error and return nothing.
        else:
            if transaction_type not in [
                "TRADE",
                "RECEIVE_AND_DELIVER",
                "DIVIDEND_OR_INTEREST",
                "ACH_RECEIPT",
                "ACH_DISBURSEMENT",
                "CASH_RECEIPT",
                "CASH_DISBURSEMENT",
                "ELECTRONIC_FUND",
                "WIRE_OUT",
                "WIRE_IN",
                "JOURNAL",
                "MEMORANDUM",
                "MARGIN_CALL",
                "MONEY_MARKET",
                "SMA_ADJUSTMENT",
            ]:
                self.get_logger_adapter().error(
                    "The type of transaction type you specified is not valid."
                )
                raise ValueError("Bad Input")

        # if transaction_id is not none, it means we need to make a request to the get_transaction endpoint.
        if transaction_id:
            endpoint = f"trader/v1/accounts/{account}/transactions/{transaction_id}"
            return self._make_request(method="get", endpoint=endpoint)

        # if it isn't then we need to make a request to the get_transactions endpoint.
        else:
            params = {
                "types": transaction_type_tuple,
                "symbol": symbol,
                "startDate": start_date_tuple,
                "endDate": end_date,
            }
            self.get_logger_adapter().info(f"get transaction params: {params}")
            if account is None and self.account_number:
                account = self.account_number

            endpoint = f"trader/v1/accounts/{account}/transactions"
            return self._make_request(method="get", endpoint=endpoint, params=params)

    def get_preferences(self) -> UserPreferencesResponse:
        """Get's User Preferences for a specific account.
        ### Documentation:
        ----
        ### Arguments:
        ----
        None. Get user preference information for the logged in user.
        ### Usage:
        ----
            >>> schwab_client.get_preferences()
        ### Returns:
        ----
            Perferences dictionary
        """
        # define the endpoint
        endpoint = "trader/v1//userPreference"
        # return the response of the get request.
        resp_data = self._make_request(method="get", endpoint=endpoint)
        return cast(UserPreferencesResponse, resp_data)

    def search_instruments(self, symbol: str, projection: Optional[str] = None) -> Dict:
        """Search or retrieve instrument data, including fundamental data.

        ### Documentation:
        ----
        ### Arguments:
        ----
        symbol: The symbol of the financial instrument you would
            like to search.

        projection: The type of request,
        Available values : symbol-search, symbol-regex, desc-search, desc-regex, search, fundamental default is "symbol-search".
                1. symbol-search
                Retrieve instrument data of a specific symbol or cusip

            2. symbol-regex
                Retrieve instrument data for all symbols matching regex.
                Example: symbol=XYZ.* will return all symbols beginning with XYZ

            3. desc-search
                Retrieve instrument data for instruments whose description contains
                the word supplied. Example: symbol=FakeCompany will return all
                instruments with FakeCompany in the description

            4. desc-regex
                Search description with full regex support. Example: symbol=XYZ.[A-C]
                returns all instruments whose descriptions contain a word beginning
                with XYZ followed by a character A through C

            5. search


            6 fundamental

                Returns fundamental data for a single instrument specified by exact symbol.

        ### Usage:
        ----
            >>> schwab_client.search_instrument(
                    symbol='XYZ',
                    projection='symbol-search'
                )
            >>> schwab_client.search_instrument(
                    symbol='XYZ.*',
                    projection='symbol-regex'
                )
            >>> schwab_client.search_instrument(
                    symbol='FakeCompany',
                    projection='desc-search'
                )
            >>> schwab_client.search_instrument(
                    symbol='XYZ.[A-C]',
                    projection='desc-regex'
                )
            >>> schwab_client.search_instrument(
                    symbol='XYZ.[A-C]',
                    projection='fundamental'
                )
        """

        # build the params dictionary
        params = {"symbol": symbol, "projection": projection}

        # define the endpoint
        endpoint = "marketdata/v1/instruments"

        # return the response of the get request.
        return self._make_request(method="get", endpoint=endpoint, params=params)

    def get_instruments(self, cusip_id: str) -> Dict:
        """Searches an Instrument.

        Get an instrument by CUSIP (Committee on Uniform Securities Identification Procedures) code.

        ### Documentation:
        ----
        ht-tps://developer.tdamer-itrade.com/instruments/apis/get/instruments/%7Bcusip%7D

        ### Arguments:
        ----
        cusip: The CUSIP co-de of a given financial instrument.

        ### Usage:
        ----
            >>> schwab_client.get_instruments(
                cusip='SomeCUSIPNumber'
            )
        """

        # define the endpoint
        endpoint = f"marketdata/v1/instruments/{cusip_id}"

        # return the response of the get request.
        return self._make_request(method="get", endpoint=endpoint)

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["OPTION"]], date: Optional[str] = None
    ) -> OptionOnlyResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["BOND"]], date: Optional[str] = None
    ) -> BondOnlyResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["FUTURE"]], date: Optional[str] = None
    ) -> FutureOnlyResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["EQUITY"]], date: Optional[str] = None
    ) -> EquityOnlyResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["BOND", "EQUITY"]], date: Optional[str] = None
    ) -> BondEquityResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["EQUITY", "BOND"]], date: Optional[str] = None
    ) -> BondEquityResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["BOND", "FUTURE"]], date: Optional[str] = None
    ) -> BondFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["FUTURE", "BOND"]], date: Optional[str] = None
    ) -> BondFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["BOND", "OPTION"]], date: Optional[str] = None
    ) -> BondOptionResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["OPTION", "BOND"]], date: Optional[str] = None
    ) -> BondOptionResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["EQUITY", "FUTURE"]], date: Optional[str] = None
    ) -> EquityFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["FUTURE", "EQUITY"]], date: Optional[str] = None
    ) -> EquityFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["OPTION", "FUTURE"]], date: Optional[str] = None
    ) -> OptionFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["FUTURE", "OPTION"]], date: Optional[str] = None
    ) -> OptionFutureResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["OPTION", "EQUITY"]], date: Optional[str] = None
    ) -> OptionEquityResponse: ...

    @overload
    def get_market_hours(
        self, markets: Sequence[Literal["EQUITY", "OPTION"]], date: Optional[str] = None
    ) -> OptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "EQUITY", "FUTURE"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "FUTURE", "EQUITY"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "EQUITY", "BOND"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "BOND", "EQUITY"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "BOND", "FUTURE"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "FUTURE", "BOND"]],
        date: Optional[str] = None,
    ) -> BondEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "EQUITY", "OPTION"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "OPTION", "EQUITY"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "EQUITY", "BOND"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "BOND", "EQUITY"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "BOND", "OPTION"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "OPTION", "BOND"]],
        date: Optional[str] = None,
    ) -> BondOptionEquityResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "FUTURE", "OPTION"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "OPTION", "FUTURE"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "FUTURE", "BOND"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "BOND", "FUTURE"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "BOND", "OPTION"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "OPTION", "BOND"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "FUTURE", "OPTION"]],
        date: Optional[str] = None,
    ) -> BondOptionFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "OPTION", "FUTURE"]],
        date: Optional[str] = None,
    ) -> OptionEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "FUTURE", "EQUITY"]],
        date: Optional[str] = None,
    ) -> OptionEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "EQUITY", "FUTURE"]],
        date: Optional[str] = None,
    ) -> OptionEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "EQUITY", "OPTION"]],
        date: Optional[str] = None,
    ) -> OptionEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "OPTION", "EQUITY"]],
        date: Optional[str] = None,
    ) -> OptionEquityFutureResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "OPTION", "EQUITY", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "OPTION", "BOND", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "BOND", "EQUITY", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "BOND", "OPTION", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "EQUITY", "BOND", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["FUTURE", "EQUITY", "OPTION", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "FUTURE", "EQUITY", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "FUTURE", "BOND", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "BOND", "EQUITY", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "BOND", "FUTURE", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "EQUITY", "BOND", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["OPTION", "EQUITY", "FUTURE", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "FUTURE", "OPTION", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "FUTURE", "BOND", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "BOND", "FUTURE", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "BOND", "OPTION", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "OPTION", "BOND", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["EQUITY", "OPTION", "FUTURE", "BOND"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "FUTURE", "OPTION", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "FUTURE", "EQUITY", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "OPTION", "FUTURE", "EQUITY"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "OPTION", "EQUITY", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "EQUITY", "OPTION", "FUTURE"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    @overload
    def get_market_hours(
        self,
        markets: Sequence[Literal["BOND", "EQUITY", "FUTURE", "OPTION"]],
        date: Optional[str] = None,
    ) -> AllMarketsResponse: ...

    def get_market_hours(
        self, markets: Sequence[str], date: Optional[str] = None
    ) -> MarketHoursResponse:
        """Returns the hours for a specific market.

        Serves as the mechanism to make a request to the "Get Hours for Multiple Markets" and
        "Get Hours for Single Markets" Endpoint. If one market is provided a "Get Hours for Single Markets"
        request will be made and if more than one item is provided then a "Get Hours for Multiple Markets"
        request will be made.

        ### Documentation:
        ----
        ### Arguments:
        ----
        markets: The markets for which you're requesting market hours,
            comma-separated. Valid markets are:
            EQUITY, OPTION, FUTURE, BOND, or FOREX.

        date: Valid date range is from current date to 1 year from today.
        It will default to current day if not entered. Date format:YYYY-MM-DD

        ### Usage:
        ----
            >>> schwab_client.get_market_hours(markets=['EQUITY'], date='2019-10-19')
            >>> schwab_client.get_market_hours(markets=['EQUITY','FOREX'], date='2019-10-19')
        """

        # because we have a list argument, prep it for the request.
        markets = self._prepare_arguments_list(parameter_list=markets)

        # build the params dictionary
        params = {"markets": markets, "date": date}

        # define the endpoint
        endpoint = "marketdata/v1/markets"

        data = self._make_request(method="get", endpoint=endpoint, params=params)
        return cast(MarketHoursResponse, data)

    def get_movers(
        self,
        symbol_id: str,
        sort: Optional[str] = None,
        frequency: Optional[int] = None,
    ) -> Dict:
        """Gets Active movers for a specific Index.

        Top 10 (up or down) movers by value or percent for a particular market.

        ### Documentation:
        ----
        ### Arguments:
        ----
        symbol_id:
        Available values : $DJI, $COMPX, $SPX, NYSE, NASDAQ, OTCBB,
        INDEX_ALL, EQUITY_ALL, OPTION_ALL, OPTION_PUT, OPTION_CALL

        sort: Sort by a particular attribute
        Available values : VOLUME, TRADES, PERCENT_CHANGE_UP, PERCENT_CHANGE_DOWN

        frequency: To return movers with the specified directions of up or down
        Available values : 0, 1, 5, 10, 30, 60

        ### Usage:
        ----
            >>> schwab_client.get_movers(
                    symbol_id='$DJI',
                    sort ='PERCENT_CHANGE_UP',
                    frquency = 10
                )
            >>> schwab_client.get_movers(
                    symbol_id='$COMPX',
                    sort='VOLUME',
                    change='percent'
                )
        """

        # build the params dictionary
        params = {"sort": sort, "frequency": frequency}

        # define the endpoint
        endpoint = f"marketdata/v1/movers/{symbol_id}"

        # return the response of the get request.
        return self._make_request(method="get", endpoint=endpoint, params=params)

    def get_price_history(
        self,
        symbol: str,
        frequency_type: str,
        period_type: str,
        frequency: int,
        period: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        need_extended_hours_data: bool = True,
        need_previous_close: bool = True,
    ) -> Dict:
        """Gets historical candle data for a financial instrument.

        ### Arguments:
        ----
        symbol:str The ticker symbol to request data for.

        period_type:str The type of period to show.
            Valid values are day, month, year, or
            ytd (year to date). Default is day.

        period:int The number of periods to show.
        If the periodType is
        • day - valid values are 1, 2, 3, 4, 5, 10
        • month - valid values are 1, 2, 3, 6
        • year - valid values are 1, 2, 3, 5, 10, 15, 20
        • ytd - valid values are 1

        If the period is not specified and the periodType is
        • day - default period is 10.
        • month - default period is 1.
        • year - default period is 1.
        • ytd - default period is 1.

        start_date:int Start date as milliseconds
            since epoch.
        If not specified startDate will be (endDate - period) excluding weekends and holidays.

        end_date:int End date as milliseconds
            since epoch.
        If not specified, the endDate will default to the market close of previous business day.

        frequency_type:str The time frequencyType

        The type of frequency with
        which a new candle is formed.
        Available values : minute, daily, weekly, monthly
        If the periodType is
        • day - valid value is minute
        • month - valid values are daily, weekly
        • year - valid values are daily, weekly, monthly
        • ytd - valid values are daily, weekly

        If frequencyType is not specified, default value depends on the periodType
        • day - defaulted to minute.
        • month - defaulted to weekly.
        • year - defaulted to monthly.
        • ytd - defaulted to weekly.


        frequency:int The number of the frequency type
            to be included in each candle.
        The time frequency duration

        If the frequencyType is
        • minute - valid values are 1, 5, 10, 15, 30
        • daily - valid value is 1
        • weekly - valid value is 1
        • monthly - valid value is 1

        If frequency is not specified, default value is 1

        needExtendedHoursData:bool -> Need extended hours data?
        extended_hours: True to return extended hours
            data, false for regular market hours only.
            Default is true

        needPreviousClose:bool -> Need previous close price/date?

        """

        # Fail early, can't have a period with start and end date specified.
        if start_date and end_date and period:
            raise ValueError("Cannot have Period with start date and end date")

        # Check only if you don't have a date and do have a period.
        elif not start_date and not end_date and period:
            # Attempt to grab the key, if it fails we know there is an error.
            # check if the period is valid.
            if int(period) in VALID_CHART_VALUES[frequency_type][period_type]:
                pass
            else:
                raise IndexError("Invalid Period.")

            if frequency_type == "minute" and int(frequency) not in [1, 5, 10, 15, 30]:
                raise ValueError("Invalid Minute Frequency, must be 1,5,10,15,30")

        # build the params dictionary
        params = {
            "symbol": symbol,
            "periodType": period_type,
            "period": period,
            "startDate": start_date,
            "endDate": end_date,
            "frequency": frequency,
            "frequencyType": frequency_type,
            "needExtendedHoursData": need_extended_hours_data,
            "needPreviousClose": need_previous_close,
        }

        # define the endpoint
        endpoint = "marketdata/v1/pricehistory"

        # return the response of the get request.
        return self._make_request(method="get", endpoint=endpoint, params=params)

    def get_options_chain(
        self, option_chain: Union[Dict, OptionsChainParams]
    ) -> OptionsChainResponse:
        """Returns Option Chain Data and Quotes.

        Get option chain for an optionable Symbol using one of two methods. Either,
        use the OptionChain object which is a built-in object that allows for easy creation
        of the POST request. Otherwise, can pass through a dictionary of all the
        arguments needed.

        ### Documentation:
        ----
        ht-tps://developer.tdamer-itrade.com/option-chains/apis/get/marketdata/chains

        ### Arguments:
        ----
        option_chain: Represents a dicitonary containing values to
            query.

        ### Usage:
        ----
            >>> schwab_client.get_options_chain(
                option_chain={'key1':'value1'}
            )
        """

        # First check if it's an `OptionChain` object.
        if isinstance(option_chain, OptionsChainParams):
            # If it is, then grab the params.
            params = option_chain.query_parameters

        else:
            # Otherwise just take the raw dictionary.
            params = option_chain

        # define the endpoint
        endpoint = "marketdata/v1/chains"

        # return the response of the get request.
        return cast(
            OptionsChainResponse,
            self._make_request(
                method="get",
                endpoint=endpoint,
                params=params,
                incl_fetch_time=True,
                incl_response=True,
            ),
        )

    def get_expiration_chain(self, symbol: str) -> ExpirationChainResponse:
        """
        Get Option Expiration (Series) information for an optionable symbol.
        Does not include individual options contracts for the underlying.
        """
        params = {"symbol": symbol}

        endpoint = "marketdata/v1/expirationchain"

        return cast(
            ExpirationChainResponse,
            self._make_request(method="get", endpoint=endpoint, params=params),
        )

    """
    -----------------------------------------------------------
    MISC
    -----------------------------------------------------------
    """

    @staticmethod
    def _utcformat(dt, timespec="milliseconds"):
        """
        Convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
        Like Schwab wants.
        This is needed for   /accounts/{accountNumber}/orders,  /orders, and /accounts/{accountNumber}/transactions
        """
        iso_str = dt.astimezone(dt.timezone.utc).isoformat("T", timespec)
        if iso_str.endswith("+00:00"):
            iso_str = iso_str[:-6] + "Z"  # Replace the last 6 characters with "Z"
        return iso_str

    def _extract_redirect_code(self, redirect_url: str) -> str:
        if "code=" in redirect_url and "&session=" in redirect_url:
            try:
                start_index = redirect_url.index("code=") + len("code=")
                end_index = redirect_url.index("&session=")
                # # Extract substring between "code=" and "&session="
                # extracted_text = redirect_url[start_index:end_index]
                # # URL decode the extracted text
                # extracted_code = urllib.parse.unquote(extracted_text)
                extracted_code = urllib.parse.unquote(
                    redirect_url[start_index:end_index]
                )
                return extracted_code
            except ValueError as ve:
                self.get_logger_adapter().error(
                    f"There's a problem with the redirect url 'code' {ve}"
                )
                return ""
        else:
            self.get_logger_adapter().error(
                f"There's a problem with the redirect url 'code' {redirect_url}"
            )
            return ""

    @classmethod
    def from_credentials(cls, credentials: "SchwabCredentials"):
        if os.path.exists(credentials.token_path):
            try:
                with open(credentials.token_path, "r") as f:
                    token_data = json.load(f)
            except json.JSONDecodeError:
                if not token_data:
                    os.remove(credentials.token_path)
        return cls(credentials=credentials)

    @classmethod
    def from_client_idx(cls, idx: int):
        """
        :param idx: account index
                    must be between 1 and number of schwab accounts
        """
        credentials = get_credentials(idx)
        return cls.from_credentials(credentials)

    def __repr__(self):
        return "<{}.{}<{}> @ {}>".format(
            # self.__module__, type(self).__name__, self.idx, hex(id(self))
            type(self).__module__,
            type(self).__name__,
            self.idx,
            hex(id(self)),
        )
