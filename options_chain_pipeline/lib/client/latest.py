import asyncio
import datetime as dt
import json
import threading
from types import TracebackType
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING
from typing import Union

from daily.utils.logging import get_logger

from .base import BaseSchwabClient
from .capacity import CapacityLimiterMixin
from ..credentials.functions import get_credentials
from .. import exceptions as exc
from .meta import SchwabClientMeta
from daily.utils.requests_dataclasses import _extract_params_from_url

if TYPE_CHECKING:
    from ..credentials import SchwabCredentials
    from ..option_chain import OptionChain as OptionsChainParams
    import requests


logger = get_logger(__name__, level="INFO", ch=True, fh=True, fh_level="DEBUG")


class SchwabClient(CapacityLimiterMixin, BaseSchwabClient, metaclass=SchwabClientMeta):

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        credentials_path: str,
        idx: int,
        account_number: Optional[str] = None,
        auth_flow: str = "default",
        _do_init: bool = True,
    ) -> None:
        BaseSchwabClient.__init__(
            self,
            client_id,
            client_secret,
            redirect_uri,
            credentials_path,
            account_number,
            auth_flow,
            _do_init,
        )
        CapacityLimiterMixin.__init__(self)
        self.idx = idx
        self._lock = threading.RLock()
        self._alock = asyncio.Lock()
        # self._queued_tasks = []
        self._entered: bool = False

    def __enter__(self):
        self._lock.acquire()
        self._entered = True
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        trace: Optional[TracebackType],
    ):
        self._lock.release()
        self._entered = False

    async def __aenter__(self):
        await self._alock.acquire()
        self._entered = True
        return self

    async def __aexit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        trace: Optional[TracebackType],
    ):
        self._alock.release()
        self._entered = False

    def _state_manager(self, action: str) -> None:
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
        super()._state_manager(action)

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
            return True
        else:
            # no credentials file or the refresh_token expired -> triggers the oauth flow
            ret = self.oauth()
            self.authstate = ret
            return ret

    def oauth(self) -> bool:
        # called by login(no credentials file) and validate_tokens (a token is expired) or _make_request if response not OK
        """Runs the oAuth process for the Broker API."""
        # Create the Auth URL.
        url = f"{self.config['auth_endpoint']}client_id={self.client_id}&redirect_uri={self.redirect_uri}"

        print(f"Please go to URL provided to authorize your account: {url}")
        # Paste it back and send it to exchange_code_for_token.
        redirect_url = input("Paste the full URL redirect here: ")

        self.get_logger().debug(f"redirect_url: {redirect_url}")
        self.code = self._extract_redirect_code(redirect_url)
        self.get_logger().debug(f"self.code: {self.code}")

        # Exchange the Auth Code for an Access Token.
        return self.exchange_code_for_token()

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
            logger.info(
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
                logger.info("Grabbing new access token...")
                # print("Grabbing new access token...")
                self.grab_access_token()
            return True
        else:
            # token expire times are not in self.state
            ret = self.oauth()
            return ret

    def _make_request(
        self,
        method: str,
        endpoint: str,
        mode: Optional[str] = None,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
        order_details: bool = False,
        incl_fetch_time: bool = False,
        multi: Optional[int] = None,
        incl_response: bool = False,
    ) -> Dict:
        url = self._api_endpoint(endpoint=endpoint)
        # Make sure the token is valid if it's not a Token API call.
        self.validate_tokens()
        headers = self._create_request_headers(mode=mode)

        # Re-use session.
        request_session = self.request_session or requests.Session()

        # Define a new request.
        request_request = requests.Request(
            method=method.upper(),
            headers=headers,
            url=url,
            params=params,
            data=data,
            json=json,
        ).prepare()
        # Send the request.

        # capture fetch time
        fetch_time = dt.datetime.now()
        try:
            response = request_session.send(request=request_request, timeout=15)
            fetch_time = fetch_time + response.elapsed
        except requests.exceptions.ReadTimeout as e:
            raise exc.ReadTimeoutError(
                message=e,
                fetch_time=fetch_time,
                prepared_request=request_request,
            ) from e

        ts = fetch_time.timestamp()
        # report fetch time(s) to redis
        if multi is not None:
            rts = [ts for _ in range(multi)]
            self.add_request_timestamps(rts)
        else:
            self.add_request_timestamp(ts)

        if (
            not response.ok
            and "refresh" in response.text
            and "expired" in response.text
        ):
            # already passed validate_tokens for expirations so calculated time must be off...?
            self.get_logger().error(f"make_requests error = {response.text}")
            try:
                self.get_logger().error("oauth called from _make_request")
                self.oauth()
            except Exception as e:
                raise exc.UnexpectedTokenAuthError(
                    message=response.text,
                    fetch_time=fetch_time,
                    response=response,
                ) from e

        if not response.ok:
            resp_json = {}
            if incl_fetch_time:
                resp_json["fetchTime"] = fetch_time.isoformat()
            if incl_response:
                resp_json["response"] = response
                try:
                    self._handle_request_error(response, request_request)
                except Exception as e:
                    self.get_logger().error(e)
                    resp_json["error"] = e
            return resp_json

        # else:  # Then response is OK
        # Return response data
        elif order_details:

            status_code = response.status_code
            response_headers = response.headers

            # Grab the order id, if it exists.
            if "Location" in response_headers:
                order_id = response_headers["Location"].split("orders/")[1]
            else:
                order_id = ""

            return {
                "order_id": order_id,
                "headers": response.headers,
                "content": response.content,
                "status_code": status_code,
                "request_body": request_request.body,
                "request_method": request_request.method,
            }
        else:

            resp_json = response.json()
            if incl_fetch_time:
                resp_json["fetchTime"] = fetch_time.isoformat()
            if incl_response:
                resp_json["response"] = response
            return resp_json

    def _handle_request_error(
        self, response: requests.Response, request: requests.PreparedRequest
    ):
        response_text = response.text
        params = _extract_params_from_url(request.url)
        endpoint = request.path_url.split("?")[0]
        # response_text = (
        #     f"{response.text[:-1]},\"params\":{json.dumps(params)},\"path_url\":{json.dumps(request.path_url)}"
        #     + "}"
        # )
        response_text = (
            f"{response.text[:-1]},\"params\":{json.dumps(params)},\"endpoint\":{json.dumps(endpoint)}"
            + "}"
        )
        # message = f"[{response.status_code}]: {response.text}"
        message = f"[{response.status_code}]: {response_text}"
        if response.status_code == 400:
            raise exc.NotNulError(message=message)
        elif response.status_code == 401:
            raise exc.TknExpError(message=message)
        elif response.status_code == 403:
            raise exc.ForbidError(message=message)
        elif response.status_code == 404:
            raise exc.NotFndError(message=message)
        elif response.status_code == 429:
            raise exc.ExdLmtError(message=message)
        elif response.status_code in (500, 503):
            raise exc.ServerError(message=message)
        elif response.status_code > 400:
            raise exc.GeneralError(message=message)
        else:
            raise exc.GeneralError(message=message)

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

    def get_options_chain(
        self, option_chain: Union[Dict, "OptionsChainParams"]
    ) -> Dict:
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
        return self._make_request(
            method="get",
            endpoint=endpoint,
            params=params,
            incl_fetch_time=True,
            incl_response=True,
        )

    @classmethod
    def from_credentials(cls, credentials: "SchwabCredentials") -> "SchwabClient":
        return cls(
            credentials.api_key,
            credentials.secret,
            credentials.redirect_uri,
            credentials.token_path,
            credentials.idx,
            credentials.account_number,
        )

    @classmethod
    def from_account_idx(cls, idx: int) -> "SchwabClient":
        """
        :param idx: account index
                    must be between 1 and number of schwab accounts
        """
        credentials = get_credentials(idx)
        return cls.from_credentials(credentials)

    class ApiEndpoint:

        class MarketData:
            HOME = "marketdata/v1"
            OptionsChain = "marketdata/v1/chains"
            ExpirationChain = "marketdata/v1/expirationchain"
            Quotes = "marketdata/v1/quotes"
            Preferences = "marketdata/v1/userPreference"
            SearchInstruments = "marketdata/v1/instruments"
            GetInstruments = "marketdata/v1/instruments/{cusip_id}"
            """cusip_id"""
            MarketHours = "marketdata/v1/markets"
            Movers = "marketdata/v1/movers/{symbol_id}"
            """symbol_id: '$COMPX', '$DJI', etc."""
            PriceHistory = "marketdata/v1/pricehistory"
