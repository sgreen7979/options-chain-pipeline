import datetime as dt
import json
import threading

# import requests
import time
from typing import Callable

# from typing import cast
from typing import Dict
from typing import List

# from typing import Literal
from typing import Optional
from typing import Union

# from typing import TYPE_CHECKING

from redis import Redis

# from .types import ClientProto
# from .. import exceptions as exc
from ..option_chain import OptionChain as OptionsChainParams

# if TYPE_CHECKING:
# import asyncio

redis = Redis()


# class CapacityLimiterMixin(ClientProto):
class CapacityLimiterMixin:

    service_map = {
        "accounts": "get_accounts",
        "cancel_order": "cancel_order",
        "expiration_chain": "get_expiration_chain",
        "get_instruments": "get_instruments",
        "market_hours": "get_market_hours",
        "modify_order": "modify_order",
        "movers": "get_movers",
        "options_chain": "get_options_chain",
        "order": "get_order",
        "orders_path": "get_orders_path",
        "place_order": "place_order",
        "preferences": "get_preferences",
        "price_history": "get_price_history",
        "quotes": "get_quotes",
        "search_instruments": "search_instruments",
        "transactions": "get_transactions",
    }
    service_map_params = {
        "get_accounts": (
            ("account", Optional[str]),
            ("fields", Optional[str]),
        ),
        "cancel_order": (
            ("account", str),
            ("order_id", str),
        ),
        "get_expiration_chain": (("symbol", str)),
        "get_instruments": (("cusip_id", str),),
        "get_market_hours": (
            ("markets", List[str]),
            ("date", Optional[str]),
        ),
        "modify_order": (
            ("account", str),
            ("order", Dict),
            ("order_id", str),
        ),
        "get_movers": (
            ("symbol_id", str),
            ("sort", Optional[str]),
            ("frequency", Optional[int]),
        ),
        "get_options_chain": (("symbol", str)),
        "get_order": (
            ("account", str),
            ("order_id", str),
        ),
        "get_orders_path": (
            ("account", Optional[str]),
            ("max_results", Optional[int]),
            ("from_entered_time", Optional[str]),
            ("to_entered_time", Optional[str]),
            ("status", Optional[str]),
        ),
        "place_order": (
            ("account", str),
            ("order", Dict),
        ),
        "get_preferences": None,
        "get_price_history": (
            ("symbol", str),
            ("frequency_type", str),
            ("period_type", str),
            ("frequency", int),
            ("period", Optional[int]),
            ("start_date", Optional[str]),
            ("end_date", Optional[str]),
            ("need_extended_hours_data", Optional[bool]),
            ("need_previous_close", Optional[bool]),
        ),
        "get_quotes": (
            ("instruments", List[str]),
            ("fields", Optional[List[str]]),
            ("indicative", bool),
        ),
        "search_instruments": (
            ("symbol", str),
            ("projection", Optional[str]),
        ),
        "get_transactions": (
            ("account", str),
            ("transaction_type", Optional[str]),
            ("symbol", Optional[str]),
            ("start_date", Optional[str]),
            ("end_date", Optional[str]),
            ("transaction_id", Optional[str]),
        ),
    }

    def get_request_times(self) -> List[float]:
        """Returns a list of timestamps of when requests were made within
        the last 60 seconds
        """
        self._purge_request_times()
        return [float(t) for t in redis.lrange(self.redis_key_req_times, 0, -1)]

    def has_capacity(self, incl_queued: bool = False) -> bool:
        return self.get_capacity(incl_queued) > 0

    def get_wait_time(self, incl_queued: bool = False) -> float:
        """Returns the number of seconds until client will have capacity"""
        return max(self._get_next_capacity_timestamp(incl_queued) - time.time(), 0.0)
        # if self.has_capacity(incl_queued):
        #     return 0.0
        # else:
        #     idx = 0 if not incl_queued else self.capacity_queued - 1
        #     self._purge_request_times()
        #     earliest_req_ts = redis.lindex(self.redis_key_req_times, idx)
        #     if earliest_req_ts is None:
        #         return 0.0

        #     earliest_req_ts = float(earliest_req_ts)
        #     return max(earliest_req_ts + 60.0 - time.time(), 0.0)

    def get_next_timestamp(self, incl_queued: bool = False) -> float:
        """Returns the timestamp of when the client will next have capacity"""
        return self._get_next_capacity_timestamp(incl_queued)
        # if self.has_capacity(incl_queued):
        #     return time.time()
        # else:
        #     idx = 0 if not incl_queued else self.capacity_queued - 1
        #     self._purge_request_times()
        #     earliest_req_ts = redis.lindex(self.redis_key_req_times, idx)
        #     if earliest_req_ts is None:
        #         return time.time()
        #     earliest_req_ts = float(earliest_req_ts)
        #     return earliest_req_ts + 60.0

    def get_next_datetime(self, incl_queued: bool = False) -> "dt.datetime":
        """Returns the datetime of when the client will next have capacity"""
        return dt.datetime.fromtimestamp(self._get_next_capacity_timestamp(incl_queued))
        # return cast(dt.datetime, self._get_next_capacity_time(incl_queued, "datetime"))
        # if self.has_capacity(incl_queued):
        #     return dt.datetime.now()
        # else:
        #     idx = 0 if not incl_queued else self.capacity_queued - 1
        #     self._purge_request_times()
        #     earliest_req_ts = redis.lindex(self.redis_key_req_times, idx)
        #     if earliest_req_ts is None:
        #         return dt.datetime.now()
        #     earliest_req_ts = float(earliest_req_ts)
        #     earliest_req_dt = dt.datetime.fromtimestamp(earliest_req_ts)
        #     return earliest_req_dt + dt.timedelta(seconds=60.0)

    def _get_next_capacity_timestamp(
        self,
        incl_queued: bool = False,
    ) -> float:
        """Timestamp at which point capacity will be available"""
        if self.has_capacity(incl_queued):
            return time.time()
        else:
            idx = 0 if not incl_queued else self.capacity_queued - 1
            self._purge_request_times()
            earliest_req_ts = redis.lindex(self.redis_key_req_times, idx)
            if earliest_req_ts is None:
                return time.time()
            earliest_req_ts = float(earliest_req_ts)
            return earliest_req_ts + 60.0

    # FIXME: for capacity_queued
    def get_capacity_at(
        self,
        t: Union[float, "dt.datetime"],
        # future_req_times: Optional[List[Union[float, "dt.datetime"]]] = None,
        incl_queued: bool = False,
    ) -> int:
        """Returns the projected capacity at a future time

        :param t: the future time
        :type t: Union[float, dt.datetime]
        :param future_req_times: a list of future request times to include, defaults to None
        :type future_req_times: Optional[List[Union[float, dt.datetime]]], optional
        :return: the projected capacity at time t
        :rtype: int
        """
        # if self.has_capacity(incl_queued):
        #     return self.rolling_capacity

        if isinstance(t, dt.datetime):
            ts = t.timestamp()
        else:
            ts = t

        # capacity_used = self.capacity_used
        # if incl_queued:
        #     capacity_queued = self.capacity_queued
        #     capacity_used += capacity_queued

        request_times = self.get_request_times()
        cutoff = ts - 60.0
        filtered_request_times = [rt for rt in request_times if rt >= cutoff]
        capacity_used = len(filtered_request_times)

        purged = [rt for rt in request_times if rt < cutoff]
        purged_count = len(purged)

        proj_capacity = self.rolling_capacity - capacity_used
        if incl_queued:
            capacity_queued = self.capacity_queued
            for i in range(capacity_queued):
                if (i + 1 > purged_count) or (purged[i] + 60.0 > ts):
                    break
                proj_capacity -= 1
        return proj_capacity
        # return proj_capacity
        # else:
        # return self.rolling_capacity - purged_count

        # next_availability = self.get_next_capacity_ts(incl_queued)

        # return self.rolling_capacity - len(req_times)  # type: ignore

    def _purge_request_times(self) -> int:
        # Use a Lua script to atomically remove old timestamps
        lua_script = """
        local key = KEYS[1]
        local one_minute_ago = tonumber(ARGV[1])
        while true do
            local timestamp = redis.call('lindex', key, 0)
            if not timestamp then break end
            if tonumber(timestamp) >= one_minute_ago then break end
            redis.call('lpop', key)
        end
        return true
        """
        one_minute_ago = time.time() - 60
        return redis.eval(lua_script, 1, self.redis_key_req_times, one_minute_ago)

    def add_request_timestamps(self, timestamps: List[float]):
        for ts in timestamps:
            redis.rpush(self.redis_key_req_times, ts)
        self._purge_request_times()

    def add_request_timestamp(self, ts: Optional[float] = None):
        """Append a timestamp to the list

        :param ts: the timestamp, optional, defaults to the current time
        """
        timestamp = ts or time.time()
        redis.rpush(self.redis_key_req_times, timestamp)
        self._purge_request_times()

    def get_capacity(self, incl_queued: bool = False) -> int:
        capacity_used = self.capacity_used
        capacity_queued = 0 if not incl_queued else self.capacity_queued
        return max(self.rolling_capacity - capacity_used - capacity_queued, 0)

    @property
    def rolling_capacity(self) -> int:
        return self.config["rolling_sixty_limit"]  # type: ignore

    @property
    def capacity_used(self):
        self._purge_request_times()
        return redis.llen(self.redis_key_req_times)

    @property
    def capacity_queued(self) -> int:
        return redis.llen(self.redis_key_req_queue)

    # def get_earliest_timestamp(self) -> Optional[float]:
    #     self._purge_request_times()
    #     if item := redis.lindex(self.redis_key_req_times, 0):
    #         return float(item)

    @property
    def redis_key(self) -> str:
        return f"schwab_client:{self.client_id}"  # type: ignore

    @property
    def redis_key_req_times(self) -> str:
        return f"{self.redis_key}:request_times"

    @property
    def redis_key_req_queue(self) -> str:
        return f"{self.redis_key}:request_queue"

    # def _make_request(
    #     self,
    #     method: str,
    #     endpoint: str,
    #     mode: Optional[str] = None,
    #     params: Optional[dict] = None,
    #     data: Optional[dict] = None,
    #     json: Optional[dict] = None,
    #     order_details: bool = False,
    #     incl_fetch_time: bool = False,
    #     multi: Optional[int] = None,
    # ) -> dict:
    #     url = self._api_endpoint(endpoint=endpoint)  # type: ignore
    #     # Make sure the token is valid if it's not a Token API call.
    #     self.validate_tokens()  # type: ignore
    #     headers = self._create_request_headers(mode=mode)  # type: ignore

    #     # Re-use session.
    #     request_session = self.request_session or requests.Session()  # type: ignore

    #     # Define a new request.
    #     request_request = requests.Request(
    #         method=method.upper(),
    #         headers=headers,
    #         url=url,
    #         params=params,
    #         data=data,
    #         json=json,
    #     ).prepare()
    #     # Send the request.

    #     # capture fetch time
    #     fetch_time = dt.datetime.now()
    #     response = request_session.send(request=request_request, timeout=15)
    #     fetch_time = fetch_time + response.elapsed
    #     ts = fetch_time.timestamp()

    #     # report fetch time(s) to redis
    #     if multi is not None:
    #         rts = [ts for _ in range(multi)]
    #         self.add_request_timestamps(rts)
    #     else:
    #         self.add_request_timestamp(ts)

    #     # grab the status code
    #     status_code = response.status_code

    #     if not response.ok:
    #         self.get_logger().error(f"make_requests error = {response.text}")  # type: ignore
    #         if "refresh" in response.text and "expired" in response.text:
    #             # already passed validate_tokens for expirations so calculated time must be off...?
    #             try:
    #                 self.get_logger().error("oauth called from _make_request")  # type: ignore
    #                 self.oauth()  # type: ignore
    #             except Exception as e:
    #                 raise exc.GeneralError(message=response.text) from e
    #         else:
    #             self._handle_request_error(response)  # type: ignore
    #     # else:  # Then response is OK
    #     response_headers = response.headers
    #     # Grab the order id, if it exists.
    #     if "Location" in response_headers:
    #         order_id = response_headers["Location"].split("orders/")[1]
    #     else:
    #         order_id = ""

    #     # Return response data
    #     if order_details:
    #         return {
    #             "order_id": order_id,
    #             "headers": response.headers,
    #             "content": response.content,
    #             "status_code": status_code,
    #             "request_body": request_request.body,
    #             "request_method": request_request.method,
    #         }
    #     else:
    #         rep_json = response.json()
    #         if incl_fetch_time:
    #             rep_json["fetchTime"] = fetch_time.isoformat()
    #         return rep_json

    # def get_quotes(
    #     self,
    #     instruments: List[str],
    #     fields: Optional[List[str]] = None,
    #     indicative: bool = False,
    # ) -> Dict:
    #     """
    #     Get quotes for specified instruments.
    #     Works for a single instrument or multiple
    #     Arguments:
    #     ----
    #     instruments {List[str]} -- List of instrument symbols.

    #     fields {Optional[List[str]]} -- List of fields to include in the quotes (default: None).
    #     Request for subset of data by passing coma separated list of root nodes,
    #     possible root nodes are:
    #     quote, fundamental, extended, reference, regular.
    #     Sending quote, fundamental in request will return quote and fundamental data in response.
    #     Dont send this attribute for full response.

    #     indicative:bool=False -- Include indicative symbol quotes for all ETF symbols in request.
    #     If ETF symbol ABC is in request and indicative=true API will return
    #     quotes for ABC and its corresponding indicative quote for $ABC.IV
    #     Available values : true, false

    #     Returns:
    #     ----
    #     {Dict} -- Dictionary containing the quotes data.
    #     """
    #     # Prepare instruments list for the request.
    #     instruments = self._prepare_arguments_list(parameter_list=instruments)  # type: ignore

    #     # Prepare fields list if provided.
    #     if fields:
    #         fields = self._prepare_arguments_list(parameter_list=fields)  # type: ignore

    #     # Build the params dictionary.
    #     params = {"symbols": instruments}
    #     if fields:
    #         params["fields"] = fields
    #     params["indicative"] = indicative  # type: ignore

    #     # Define the endpoint.
    #     endpoint = "marketdata/v1/quotes"

    #     # Return the response of the get request.
    #     return self._make_request(  # type: ignore
    #         method="get", endpoint=endpoint, params=params, multi=len(instruments)
    #     )

    def _await_capacity(self, incl_queued: bool = False):
        wait_time = self.get_wait_time(incl_queued)
        if wait_time > 0:
            time.sleep(wait_time)
        return self

    def _parse_params(self, params: Dict, method_name: str):
        if method_name == "quotes":
            return {
                "instruments": params["instruments"],
                "fields": params.get("fields"),
                "indicative": params.get("indicative", False),
            }
        elif method_name == "options_chain":
            return OptionsChainParams(**params)
        elif method_name == "search_instruments":
            return {"symbol": params["symbol"], "projection": params.get("projection")}
        elif method_name == "get_instruments":
            return {"cusip_id": params["cusip_id"]}
        elif method_name == "market_hours":
            return {"markets": params["markets"], "date": params.get("date")}
        elif method_name == "movers":
            return {
                "symbol_id": params["symbol_id"],
                "sort": params.get("sort"),
                "frequency": params.get("frequency"),
            }
        elif method_name == "price_history":
            return {
                "symbol": params["symbol"],
                "frequency_type": params["frequency_type"],
                "period_type": params["period_type"],
                "frequency": params["frequency"],
                "period": params.get("period"),
                "start_date": params.get("start_date"),
                "end_date": params.get("end_date"),
                "need_extended_hours_data": params.get(
                    "need_extended_hours_data", True
                ),
                "need_previous_close": params.get("need_previous_close", True),
            }
        elif method_name == "expiration_chain":
            return {"symbol": params["symbol"]}
        else:
            return params

    def _consume_queue(self, handler: Callable[[Dict, int], None], join: bool = False):
        def __consume():
            req = redis.lpop(self.redis_key_req_queue)
            while req is not None:
                req_dict = json.loads(req.decode())
                service = req_dict["service"]
                method_name = self.service_map[service]
                params = json.loads(req_dict["params"])
                method = getattr(self, method_name)
                run_at = dt.datetime.fromisoformat(req_dict["run_at"])
                delay = max(run_at.timestamp() - time.time(), 0.0)
                if delay > 0:
                    time.sleep(delay)
                self._await_capacity()
                data = method(params)
                handler(data, self.idx)  # type: ignore
                req = redis.lpop(self.redis_key_req_queue)

        th = threading.Thread(target=__consume)
        th.start()
        if join:
            th.join()

    # # FIXME: for new redis request_queue list implementation
    # def get_task_queue(self):
    #     return self._queued_tasks  # type: ignore

    # # FIXME: for new redis request_queue list implementation
    # @property
    # def num_queued(self):
    #     return sum([getattr(t, "length") for t in self._queued_tasks])  # type: ignore

    # # FIXME: for new redis request_queue list implementation
    # def queue_batch_task(self, bt: "asyncio.Task"):
    #     self._queued_tasks.append(bt)  # type: ignore
    #     self.get_logger().debug(f"Queued batch task {bt.get_name()}")  # type: ignore

    # # FIXME: for new redis request_queue list implementation
    # def remove_batch_task(self, bt: "asyncio.Task"):
    #     if bt not in self._queued_tasks:  # type: ignore
    #         raise RuntimeError(
    #             f"Batch task {bt.get_name()} not in task queue; created {time.time() - getattr(bt, 'create_time')}s ago"
    #         )
    #     self._queued_tasks.remove(bt)  # type: ignore
    #     self.get_logger().debug(f"Removed batch task {bt.get_name()} from queue")  # type: ignore
