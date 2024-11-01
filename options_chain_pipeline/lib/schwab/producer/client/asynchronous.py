import asyncio
from collections import defaultdict, deque
from contextlib import asynccontextmanager
import datetime as dt
import json
import threading
from time import perf_counter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    DefaultDict,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Self,
    Sequence,
    Set,
    Type,
)
from typing_extensions import deprecated

import aiohttp
from kafka import KafkaConsumer, KafkaProducer
import requests

from daily.schwab.option_chain import OptionsChainParams
from daily.utils.arange import AsyncRange
from daily.utils.logging import Formatter, add_new_fh
from daily.utils.sleep import asleep_until

from ...client.asynchronous import (
    AsyncSchwabClient,
    SessionFactoryT,
    sessionFactory,
    _SessionProto,
)
from ...api import Endpoints
from ...enums import VALID_CHART_VALUES
from ... import exceptions as exc
from ...handlers.asynctask import AsyncRequestTaskHandler
from ...orders import Order
from ...requestlib.ctx import TASK_INFO_VAR, _TaskInfo
from ...requestlib.task import AsyncRequestTaskModel
from ...requestlib.make import MakeRequestModel
from ...session.ctx import SESSION_VAR, HEADERS_VAR
from ..mixins.asynchronous import AsyncSchwabProducerMixin

# # NOTE the value of this environment variable impacts the type of
# # the `TASK_VAR``ContextVar, so we set the environment
# # variabale before the import
# os.environ["@DAILY_REQUEST_TASK_PROTOCOL"] = "async"

# from ...requestlib.ctx import TASK_VAR  # noqa: E402
from ...requestlib.ctx import get_and_set_async_request_task_var

TASK_VAR = get_and_set_async_request_task_var()

if TYPE_CHECKING:
    from ...client_pool.asynchronous import AsyncClientPool
    from ...credentials import SchwabCredentials
    from ...requestlib.summary import _SummaryModel


RequestTaskProto = AsyncRequestTaskModel[aiohttp.ClientResponse]


class AsyncSchwabProducer(AsyncSchwabClient):

    LOGGING_FILE_HANDLER_KWARGS: ClassVar[Dict[str, Any]] = dict(
        fh_level="DEBUG",
        fh_formatter=Formatter(
            "%(asctime)s %(levelname)s %(name)s<%(idx)d> %(pathname)s %(lineno)d %(message)s",
            defaults={"idx": 0},
        ),
        fh_type="RotatingFileHandler",
        fh_type_kwargs={"maxBytes": 1_048_576, "backupCount": 500_000},  # 1MB
    )

    _request_task_factory: ClassVar[
        Type[AsyncRequestTaskModel[aiohttp.ClientResponse]]
    ] = AsyncRequestTaskModel[aiohttp.ClientResponse]

    response_handler: AsyncRequestTaskHandler

    def __init__(
        self,
        idx: Optional[int] = None,
        *args,
        credentials: Optional["SchwabCredentials"] = None,
        batch_size: int = 20,
        rolling_capacity: Optional[int] = None,
        scheduling_capacity: Optional[int] = None,
        queue_capacity: Optional[int] = None,
        prep_capacity: Optional[int] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            self,
            idx,
            *args,
            credentials=credentials,
            **kwargs,
        )
        self.set_response_handler(AsyncRequestTaskHandler(self))

        from ..repository import get_repository_service

        self._repo_service = get_repository_service(
            self,
            rolling_capacity=rolling_capacity,
            scheduling_capacity=scheduling_capacity,
            queue_capacity=queue_capacity,
        )

        # Store in-progress tasks for this client
        # We use this set to rightsize our current capacity,
        # which might exclude requests in progress
        self._in_progress_tasks: Set[asyncio.Future] = set()

        self._batch_size: int = batch_size

        self._lock: asyncio.Lock = asyncio.Lock()

        self._pause_event: asyncio.Event = asyncio.Event()
        self._done_event: asyncio.Event = asyncio.Event()

        self.callbacks: DefaultDict[str, Optional[Callable]] = defaultdict(lambda: None)

        self._prep_capacity: int = prep_capacity or self._repo_service._rolling_capacity
        self._prepared: asyncio.Queue[RequestTaskProto] = asyncio.Queue()
        self._overflow: Deque[RequestTaskProto] = deque()

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._pool: Optional["AsyncClientPool"] = None

        # self._listener: KafkaConsumer = KafkaConsumer(
        #     f"AsyncSchwabProducer-{self.idx}-{dt.datetime.today().strftime('%Y%m%d')}",
        #     bootstrap_servers="localhost:9092",
        #     # request_timeout_ms=600_000,
        #     # connection_max_idle_ms=800_000,
        #     # retries=5,
        #     auto_offset_reset='earliest',
        #     enable_auto_commit=False,
        #     value_deserializer=lambda m: json.loads(m.decode('ascii')),
        # )
        # self._publisher: KafkaProducer = KafkaProducer(
        #     # f"AsyncSchwabProducer-{self.idx}-{dt.datetime.today().isoformat('%Y%m%d')}",
        #     bootstrap_servers="localhost:9092",
        #     # request_timeout_ms=600_000,
        #     # connection_max_idle_ms=800_000,
        #     retries=5,
        #     # auto_offset_reset='earliest',
        #     # enable_auto_commit=False,
        #     # value_deserializer=lambda m: json.loads(m.decode('ascii')),
        # )
        # self._kafka_listener_thread = threading.Thread(target=self._start_listener_in_new_thread)
        # self._kafka_listener_thread.start()
        self.started = self.stopped = self.closing = self.hard_stop = self.done = False

    # def confirm(self, topic: str, capacity: int, pid: int, mode: Literal["reserve", "unreserve"] = "reserve"):
    #     if mode == "reserve":
    #         self._publisher.send(topic, f"Confirming capacity reserved of {capacity} by {pid}.")
    #     else:
    #         self._publisher.send(topic, f"Confirming capacity unreserved of {capacity} by {pid}.")

    # async def listen(self):
    #     capacity_reserved = 0
    #     try:
    #         while not self.closing or not self._done_event.is_set():
    #             for msg in self._listener:
    #                 message = msg.value
    #                 match message:

    #                     case {"reserve_capacity": capacity_to_reserve, "by": pid, "confirm_to": topic}:
    #                         capacity_to_reserve = int(capacity_to_reserve)
    #                         pid = int(pid)
    #                         topic = str(topic)

    #                         async with self.get_redis() as conn:
    #                             await conn.set(self.redis_keys.reserved, capacity_to_reserve)

    #                         capacity_reserved += capacity_to_reserve
    #                         self.get_logger_adapter().info(
    #                             f"Capacity reserved for {capacity_to_reserve} requests by process {pid}."
    #                         )

    #                         th = threading.Thread(target=self.confirm, args=(topic, capacity_to_reserve, pid, "reserve"))
    #                         th.start()
    #                         th.join()

    #                     case {"unreserve_capacity": capacity_to_unreserve, "by": pid, "confirm_to": topic}:
    #                         capacity_to_unreserve = int(capacity_to_unreserve)
    #                         capacity_reserved -= capacity_to_unreserve
    #                         pid = int(pid)
    #                         topic = str(topic)

    #                         async with self.get_redis() as conn:
    #                             await conn.set(self.redis_keys.reserved, capacity_reserved)

    #                         self.get_logger_adapter().info(
    #                             f"Reserved capacity for {capacity_to_unreserve} requests by {pid} "
    #                             f"now unreserved. Current capacity reserved: {capacity_reserved}."
    #                         )

    #                         th = threading.Thread(target=self.confirm, args=(topic, capacity_to_unreserve, pid, "unreserve"))
    #                         th.start()
    #                         th.join()

    #             await asyncio.sleep(5.0)
    #     except Exception as e:
    #         self.get_logger_adapter(
    #             f"Exception raised in Kafka listener thread {e}", exc_info=True
    #         )
    #         raise
    #     finally:
    #         async with self.get_redis() as conn:
    #             await conn.set(self.redis_keys.reserved, 0)

    # def _start_listener_in_new_thread(self):
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)

    #     try:
    #         loop.run_until_complete(self.listen())
    #     except Exception as e:
    #         raise
    #     finally:
    #         loop.stop()

    def pause(self):
        if not self._pause_event.is_set():
            self._pause_event.set()

    def unpause(self):
        if self._pause_event.is_set():
            self._pause_event.clear()

    @property
    def redis_keys(self):
        return self._repo_service.redis_keys

    @property
    def batch_size(self):
        return self._batch_size

    def set_batch_size(self, n: int) -> Self:
        assert n > 0
        self._batch_size = n
        self.get_logger_adapter().info(f"Set batch size {n}")
        return self

    def set_callback(self, callback: Callable[..., Coroutine], endpoint: str) -> Self:
        """
        Set a callback for a specific endpoint.

        :param callback: The callback function to be associated.
        :type callback: Callable
        :param endpoint: The endpoint to associate the callback with.
        :type endpoint: str
        :return: The current instance to allow method chaining.
        :rtype: LiveMixin
        """
        self.callbacks[endpoint] = callback
        self.get_logger_adapter().debug(
            f"Set callback {callback.__qualname__} for endpoint {endpoint}"
        )
        return self

    @property
    def prep_capacity(self) -> int:
        """
        Get the maximum capacity of the preparation queue.

        :return: The preparation capacity.
        :rtype: int
        """
        return self._prep_capacity

    def set_prep_capacity(self, n: int) -> Self:
        assert n > 0, f"n must be greater than 0, got {n}"
        return self

    @property
    def prep_capacity_used(self):
        """
        Get the number of items currently in the preparation queue.

        :return: The size of the preparation queue.
        :rtype: int
        """
        return self._prepared.qsize()

    def get_available_prep_capacity(self) -> int:
        """
        Calculate and return the remaining capacity in the preparation queue.

        :return: The remaining preparation capacity.
        :rtype: int
        """
        return self.prep_capacity - self.prep_capacity_used

    def has_available_prep_capacity(self):
        return self.get_available_prep_capacity() > 0

    def set_rolling_capacity(self, n: int) -> Self:
        self._repo_service.set_rolling_capacity(n)
        return self

    def set_scheduling_capacity(self, n: int) -> Self:
        self._repo_service.set_scheduling_capacity(n)
        return self

    def set_queue_capacity(self, n: int) -> Self:
        self._repo_service.set_queue_capacity(n)
        return self

    @property
    def paused(self) -> bool:
        """
        Check if the event loop is frozen (event is set).

        :return: True if frozen, False otherwise.
        :rtype: bool
        """
        ret = self._pause_event.is_set()
        if ret:
            self.get_logger_adapter().debug("Paused")
        return ret

    async def _paused_check(self):
        """
        Asynchronously check if the event loop is frozen and sleep if so.
        """
        while self.paused:
            await asyncio.sleep(0.1)

    ###########################################################################
    ###########################################################################
    # AsyncRepositoryService hooks

    @property
    def executed(self):
        return self._repo_service.executed

    @property
    def scheduled(self):
        return self._repo_service.sched

    @property
    def task_db(self):
        return self._repo_service.task_db

    @property
    def queue(self):
        return self._repo_service.queue

    @property
    async def reserved(self):
        return await self._repo_service.reserved

    async def await_capacity(self):
        return await self._repo_service._await_capacity()

    async def get_next_scheduling_times(self, n: int) -> List[float]:
        return await self._repo_service._get_next_scheduling_times(n)

    async def get_wait_time(self) -> float:
        return await self._repo_service.get_wait_time()

    @property
    async def rolling_capacity(self):
        return await self._repo_service.rolling_capacity

    @property
    async def scheduling_capacity(self):
        return await self._repo_service.scheduling_capacity

    async def get_available_queue_capacity(self):
        return await self._repo_service.get_available_queue_capacity()

    async def get_available_capacities(self):
        # return await self._repo_service.get_available_capacities()

        current_capacity = await self._repo_service.get_available_executed_capacity()
        future_capacity = await self._repo_service.get_available_scheduling_capacity()

        # Subtract the number of in-progress tasks for this specific client
        in_progress = len(self._in_progress_tasks)
        adjusted_current_capacity = max(0, current_capacity - in_progress)
        # adjusted_future_capacity = max(0, future_capacity - in_progress)

        return adjusted_current_capacity, future_capacity

    async def has_capacity(self):
        return await self._repo_service.has_available_executed_capacity()

    async def get_capacity_used(self):
        return await self._repo_service.get_capacity_executed()  # type: ignore

    async def get_capacity_queued(self):
        return await self._repo_service.get_capacity_queued()  # type: ignore

    async def get_current_capacity(self):
        return await self._repo_service.get_available_executed_capacity()

    async def get_capacity_scheduled(self):
        return await self._repo_service.get_capacity_scheduled()  # type: ignore

    async def get_next_datetime(self):
        return await self._repo_service.get_next_datetime()

    async def get_next_timestamp(self):
        return await self._repo_service.get_next_timestamp()  # type: ignore

    @asynccontextmanager
    async def get_redis(self):
        async with self._repo_service.get_redis() as conn:
            yield conn

    ######################################################################
    ######################################################################
    # Metadata attrs

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> Self:
        self._loop = loop
        return self

    @property
    def loop(self):
        return self._loop

    def set_pool(self, pool: "AsyncClientPool") -> Self:
        self._pool = pool
        return self

    @property
    def pool(self):
        return self._pool

    def set_request_task_factory(self, request_task_factory) -> Self:
        type(self)._request_task_factory = request_task_factory
        return self

    def get_request_task_factory(self):
        return type(self)._request_task_factory

    def _get_callback(self, endpoint: str):
        return self.callbacks[endpoint]

    async def _get_response_from_context(self):
        task = TASK_VAR.get()

        if task.time_scheduled is not None:
            await asleep_until(task.get_time_scheduled_dt().timestamp() - 0.5)  # type: ignore

        session = SESSION_VAR.get()
        headers = HEADERS_VAR.get()

        if task.summary_json.prepared_request is None:
            task = self._prepare_request_task_from_context(task)

        url = task.summary_json.prepared_request.url
        headers = dict(list(headers.items()))
        params = task.make_request.clean_params()
        assert isinstance(url, str)

        start_dt = dt.datetime.now()
        perf_counter_start = perf_counter()
        try:
            # async with session:
            response = await session.get(
                url=url,
                headers=headers,
                params=params,
            )
        except (BaseException, Exception) as e:
            perf_counter_end = perf_counter()
            elapsed = dt.timedelta(seconds=perf_counter_end - perf_counter_start)
            task_info = _TaskInfo(start_dt=start_dt, elapsed=elapsed, error=e)
            TASK_INFO_VAR.set(task_info)
            await self._populate_task_summary()
            await self._log_fetch_times_from_context()
            self.get_logger_adapter().error(f"{e}", exc_info=True)  # type: ignore
            return task
        else:
            perf_counter_end = perf_counter()
            elapsed = dt.timedelta(seconds=perf_counter_end - perf_counter_start)
            task_info = _TaskInfo(start_dt=start_dt, elapsed=elapsed, response=response)
            TASK_INFO_VAR.set(task_info)
            await self._populate_task_summary()
            await self._log_fetch_times_from_context()
            return task
        finally:
            if self._prepared._unfinished_tasks > 0:  # type: ignore[attr-defined]
                self._prepared.task_done()
            await self.response_handler.async_handle_task()
            cb = self._get_callback(task.make_request.endpoint)
            if cb is not None:
                loop = self.loop or asyncio.get_running_loop()
                loop.create_task(cb())

    @staticmethod
    async def _populate_task_summary():
        task = TASK_VAR.get()
        task_info = TASK_INFO_VAR.get()
        task.set_time_executed(task_info.fetchTime.isoformat())
        # task.summary_json.fetchTime = task_info.fetchTime.isoformat()
        task.summary_json.set_elapsed(task_info.elapsed)
        if task_info.error is not None:
            task.set_error(task_info.error)
            # task.summary_json.set_error(task_info.error)
        # FIXME if isisnstance aiohttp.ClientResponse??
        elif task_info.response is not None:
            await task.summary_json.set_response(task_info.response)  # type: ignore

    async def _log_fetch_times_from_context(self):
        task = TASK_VAR.get()
        exc = None
        assert task.summary_json.fetchTime is not None, "fetchTime never set"
        try:
            ts = dt.datetime.fromisoformat(task.summary_json.fetchTime).timestamp()
            if (multi := task.make_request.multi) is not None:
                ts = [ts for _ in range(multi)]
            await self.executed.add(ts)
        except Exception as e:
            self.get_logger_adapter().error(
                f"Failed to report task fetch time(s) to redis {task.uuid}: {e}",
                exc_info=True,
            )
            exc = e
        finally:
            if exc is not None and isinstance(
                exc, (KeyboardInterrupt, SystemExit, OSError)
            ):
                raise exc

    async def _log_result(self):
        task = TASK_VAR.get()
        async with self.get_redis() as conn:
            data = task.summary_json.model_dump_json()
            await conn.hset(self.redis_keys.log, task.uuid, data)

    async def _fetch_one_in_context(self, task: "AsyncRequestTaskModel"):
        # await task.get_context(TASK_VAR).run(self._get_response_from_context)

        # Track the task for this client
        future = asyncio.create_task(
            task.get_context(TASK_VAR).run(self._get_response_from_context)
        )
        self._in_progress_tasks.add(future)

        # When the task is done, remove it from tracking
        future.add_done_callback(self._in_progress_tasks.discard)

        return await future

    async def _fetch_batch(self, request_tasks: List["AsyncRequestTaskModel"]):
        tasks = [self._fetch_one_in_context(rt) for rt in request_tasks]
        return await asyncio.gather(*tasks)

    def _ensure_headers_context(self) -> None:
        headers = super()._create_request_headers()
        HEADERS_VAR.set(headers)

    async def _ensure_session_context(self):
        session = await self._ensure_session()
        SESSION_VAR.set(session)

    async def batched_make_request_in_context(
        self, request_tasks: List["AsyncRequestTaskModel"]
    ) -> List["AsyncRequestTaskModel"]:
        await self._ensure_session_context()
        await self.validate_tokens()
        self._ensure_headers_context()

        prepared_request_tasks = list(
            map(self._prepare_request_task_from_context, request_tasks)
        )

        results = []

        async for i in AsyncRange(0, len(prepared_request_tasks), self.batch_size):
            batch = prepared_request_tasks[i : i + self.batch_size]
            batch_results = await self._fetch_batch(batch)
            results.extend(batch_results)

        return results

    def _prepare_request_task_from_context(
        self, request_task: "AsyncRequestTaskModel"
    ) -> "AsyncRequestTaskModel":
        prepared_request = requests.Request(
            method=request_task.make_request.method.upper(),
            headers=HEADERS_VAR.get(),
            url=request_task.url,
            params=request_task.make_request.params,
            data=request_task.make_request.data,
            json=request_task.make_request.jsonr,
        ).prepare()
        request_task.summary_json.set_prepared_request(prepared_request)
        assert request_task.summary_json.prepared_request.url, "url is None"
        return request_task

    async def grab_access_token(self) -> Optional[dict]:
        await self._ensure_session_context()
        session = SESSION_VAR.get()

        try:
            # Build the parameters of the request
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.state["refresh_token"],
            }
            async with await session.post(
                url=self.config["token_endpoint"],
                headers=self._create_request_headers(is_access_token_request=True),
                data=data,
            ) as response:
                if response.ok:
                    resp_data = await response.json()
                    self.get_logger_adapter().debug(
                        f"Grab acces token resp: {resp_data}"
                    )
                    return self._token_save(
                        token_dict=resp_data, refresh_token_from_oauth=False
                    )
                else:
                    resp_text = await response.text()
                    if "expired" in resp_text:
                        self.get_logger_adapter().error(
                            "Failed to refresh access token. Status code: "
                            f"{response.status}, Reason: {resp_text}.  "
                            "Reauthenticating..."
                        )
                        await self.oauth()  # Re-authenticate if refresh token is also expired

        except aiohttp.ClientResponseError as e:
            self.get_logger_adapter().error(
                f"Failed to refresh access token: {e}", exc_info=True
            )
        except asyncio.TimeoutError as e:
            self.get_logger_adapter().error(
                f"Failed to refresh access token: {e}", exc_info=True
            )

    # async def validate_tokens_from_context(self) -> bool:
    async def validate_tokens(self) -> bool:
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
                await self.oauth()
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
                await self.grab_access_token()
            return True
        else:
            # token expire times are not in self.state
            return await self.oauth()

    async def exchange_code_for_token(self):
        # called by oauth
        """Access token handler for AuthCode Workflow.
        This takes the authorization code parsed from
        the auth endpoint to call the token endpoint
        and obtain an access token.

        ### Returns: {bool} -- `True` if successful, `False` otherwise.
        """
        # called by validate_tokens (the access_token is expired)
        """Refreshes the current access token.

        This takes a valid refresh token and refreshes
        an expired access token. This is different from
        exchanging a code for an access token.

        Returns:
        ----
        dict or None: The token dictionary if successful, None otherwise.
        """
        # if session is not None and not session.closed:
        #     request_session = session
        # else:
        #     request_session = await self._ensure_session()

        await self._ensure_session_context()
        session = SESSION_VAR.get()

        url_code = self.code  # ?
        # Define the parameters of our access token post.
        data = {
            "grant_type": "authorization_code",
            "code": url_code,
            "redirect_uri": self.redirect_uri,
        }
        self.get_logger_adapter().debug(f"data = {data}")
        # Make the request.
        async with session.post(
            url=self.config["token_endpoint"],
            headers=self._create_request_headers(
                mode="form", is_access_token_request=True
            ),
            data=data,
        ) as response:

            if response.ok:
                resp_json = await response.json()
                self.get_logger_adapter().debug(
                    f"Exchange code for token resp: {resp_json}"
                )
                self._token_save(token_dict=resp_json, refresh_token_from_oauth=True)
                return True
            else:
                # Handle the case where the request fails
                resp_text = await response.text()
                self.get_logger_adapter().error(
                    f"Exchange_code_for_token request failed {response.status}, {resp_text}"
                )
                return False

    @staticmethod
    def _get_task_sendproto(task=None):
        task = task or TASK_VAR.get()
        return task.make_request.send_protocol

    @staticmethod
    def _get_task_data(task=None) -> str:
        task = task or TASK_VAR.get()
        if task.summary_json.response_data is not None:
            data = json.dumps(task.summary_json.response_data)
        elif task.summary_json.error is not None:
            data = str(task.summary_json.error)
        else:
            data = task.summary_json.model_dump_json()
        return data

    async def _execute_sendproto(self):
        task = TASK_VAR.get()
        if (sendproto := self._get_task_sendproto(task)) is not None:

            data = self._get_task_data(task)

            from ...requestlib.make import (
                HttpSendProtocol,
                RedisSendProtocol,
                SocketSendProtocol,
                KafkaSendProtocol,
            )

            if isinstance(sendproto, RedisSendProtocol):
                async with self._repo_service.get_redis() as conn:
                    await conn.rpush(sendproto.key, data)

            elif isinstance(sendproto, HttpSendProtocol):
                import requests

                url = sendproto.url
                requests.post(url, json={"data": data})

            elif isinstance(sendproto, SocketSendProtocol):
                import socket

                host = sendproto.host
                port = sendproto.port
                data_encoded = json.dumps(data).encode()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                    client_socket.connect((host, port))
                    message_length = f"{len(data_encoded):<10}"
                    client_socket.sendall(message_length.encode() + data_encoded)

            else:
                self.get_logger_adapter().error(
                    f"Unrecognized send protocol {type(sendproto).__name__}"
                )

    async def _get_task(self, uuid_encoded: bytes) -> Optional["AsyncRequestTaskModel"]:
        """
        Retrieve an AsyncRequestTaskModel by its UUID.

        :param uuid_encoded: The encoded UUID of the task.
        :type uuid_encoded: bytes
        :return: The retrieved task.
        :rtype: AsyncRequestTaskModel
        """
        if not isinstance(uuid_encoded, bytes):
            self.get_logger_adapter().error(
                f"ValueError: expected type bytes but got {type(uuid_encoded).__name__}. "
            )
            return None

        from ...requestlib.task import AsyncRequestTaskModel

        uuid = uuid_encoded.decode("utf-8")
        task = AsyncRequestTaskModel.get_by_uuid(uuid)
        if task is None:
            self.get_logger_adapter().debug(  # type: ignore
                f"Task {uuid} is pre-existing; creating the task obj now"
            )
            from ...requestlib.make import MakeRequestModel

            make_request = await self.task_db.get_task(uuid)
            assert make_request is not None, "Request wasn't saved to redis"
            task = AsyncRequestTaskModel(make_request=MakeRequestModel(**make_request))  # type: ignore
        return task

    async def _prepare_requests_loop(self):
        """
        Asynchronous loop to prepare tasks by fetching them from the queue and setting their futures.
        Runs in a separate thread.
        """
        while not self.closing:

            # Stop fetching new tasks if the preparation queue is full
            prep_capacity = self.get_available_prep_capacity()

            if prep_capacity <= 0:
                self.get_logger_adapter().warning(
                    "Prep capacity reached, pausing ingestion..."
                )
                await asyncio.sleep(1.0)
                continue
            else:
                async for _ in AsyncRange(prep_capacity):
                    uuid = await self.queue.popleft()
                    if uuid:
                        task = await self._get_task(uuid)
                        if task is not None:
                            await task.set_future()
                            try:
                                self._prepared.put_nowait(task)
                            except asyncio.QueueFull as e:
                                self.get_logger_adapter().error(
                                    f"prepared queue is full {e}", exc_info=True
                                )
                                self._overflow.append(task)
                                prep_capacity = 0
                                break
                            else:
                                prep_capacity -= 1

                if prep_capacity > 0 and (of := len(self._overflow)) > 0:
                    n = min(prep_capacity, of)
                    async for _ in AsyncRange(n):
                        task = self._overflow.popleft()
                        try:
                            self._prepared.put_nowait(task)
                        except asyncio.QueueFull:
                            self._overflow.append(task)
                            break

            if self.paused:
                await self._paused_check()
            await asyncio.sleep(2.0)

    async def schedule_task_in_context(
        self, task: "AsyncRequestTaskModel", when: float
    ):
        """
        Schedule a single task to run within its context at a specified time.

        :param uuid_encoded: The encoded UUID of the task.
        :type uuid_encoded: bytes
        :param when: The timestamp at which to execute the task.
        :type when: float
        """
        if TYPE_CHECKING:
            assert isinstance(self.loop, asyncio.AbstractEventLoop)

        task.set_time_scheduled(when)

        self.loop.call_at(
            self.loop.time() + 0.5,
            asyncio.create_task,
            task.get_context(TASK_VAR).run(self._get_response_from_context),
        )
        await self.scheduled.add_mapping({task.uuid: when})
        # when_dt = dt.datetime.fromtimestamp(when).isoformat()
        # self.get_logger_adapter().info(f"Scheduled {task.uuid} @ {when_dt}")  # type: ignore

    async def schedule_multiple_tasks_in_context(self, n: int):
        """
        Schedule multiple tasks to be executed within their contexts at specified times.

        :param uuids_encoded: List of encoded UUIDs.
        :type uuids_encoded: List[bytes]
        :param whens: List of timestamps corresponding to each UUID.
        :type whens: List[float]
        """
        request_tasks = self._get_prepared_tasks(n)
        whens = await self.get_next_scheduling_times(len(request_tasks))
        tasks = [
            self.schedule_task_in_context(rt, when)
            for rt, when in zip(request_tasks, whens)
        ]
        await asyncio.gather(*tasks)

    def _get_prepared_tasks(self, n: int):
        """
        Retrieve a specified number of prepared tasks from the queue.

        :param n: Number of tasks to retrieve.
        :type n: int
        :return: List of prepared tasks.
        :rtype: List[AsyncRequestTaskModel]
        """
        prepared_tasks = []
        for _ in range(n):
            try:
                request_task = self._prepared.get_nowait()
            except asyncio.QueueEmpty as e:
                self.get_logger_adapter().error(f"prepared queue is empty {e}")
                break
            else:
                prepared_tasks.append(request_task)

        return prepared_tasks

    async def _consume_queue_once(self):
        """
        Consume tasks from the queue once, executing or scheduling them based on their type and capacity.
        """

        await self.executed.purge()
        await self.scheduled.purge()

        capacities = current_capacity, future_capacity = (
            await self.get_available_capacities()
        )
        self.get_logger_adapter().info(f"Current capacities: {capacities}")
        if current_capacity <= 0 and future_capacity <= 0:
            pass

        elif current_capacity > 0:
            n = min(current_capacity, self._prepared.qsize())
            if n > 0:
                await self.batched_make_request_in_context(self._get_prepared_tasks(n))
                # self.get_logger_adapter().info(f"Executed {n} prepared request tasks")
        elif future_capacity > 0:
            n = min(future_capacity, self._prepared.qsize())
            if n > 0:
                await self.schedule_multiple_tasks_in_context(n)
                # self.get_logger_adapter().info(f"Scheduled {n} prepared request tasks")
                # async for _ in self.arange(n):
                #     self._prepared.task_done()
        await self._paused_check()
        await asyncio.sleep(1.0)

    async def consume_queue(self, join: bool = False):
        """
        Main loop to consume tasks from the queue, joining if required.

        :param join: Whether to join threads after consumption.
        :type join: bool
        """
        if join:
            self.stop()
        else:
            while self._prepared.qsize() < await self.rolling_capacity:
                await asyncio.sleep(1.0)

        while not self.closing:
            await self._consume_queue_once()

        if self.hard_stop:
            self.get_logger_adapter().critical("Hard stop")
        else:
            await self.join_queue()

        self.done = self.stopped = True
        self._done_event.set()
        self.get_logger_adapter().info("Done")

    async def join_queue(self):
        """
        Joins the queue by consuming all tasks.
        """
        while not await self.queue.empty():
            await self._consume_queue_once()

    def start(self, join: bool = False):
        """
        Start the preparation and consumption loops in separate threads.

        :param join: Whether to join the threads upon completion.
        :type join: bool
        """
        add_new_fh(self.get_logger_adapter().logger, **self.LOGGING_FILE_HANDLER_KWARGS)
        self.get_logger_adapter().info("Starting")
        self.started = True
        assert self.loop is not None, "loop was never set"
        self.loop.create_task(self._prepare_requests_loop()).add_done_callback(
            lambda f: f"prepare requests loop is exiting {f.result()}"
        )
        # asyncio.ensure_future(self._prepare_requests_loop())
        # asyncio.ensure_future(self.consume_queue(join))
        self.loop.create_task(self.consume_queue(join)).add_done_callback(
            lambda f: f"consume_queue loop is exiting {f.result()}"
        )

    def stop(self, hard_stop: bool = False) -> Self:
        """
        Stop the processing loops gracefully or immediately.

        :param hard_stop: If True, performs an immediate stop.
        :type hard_stop: bool
        :return: The current instance to allow method chaining.
        :rtype: LiveMixin
        """
        self.get_logger_adapter().info("Closing soon")  # type: ignore
        self.hard_stop = hard_stop
        self.closing = True
        return self

    async def _handle_auth_error(
        self,
        resp_text: str,
    ) -> None:
        self.get_logger_adapter().error(f"make_requests error = {resp_text}")  # type: ignore
        try:
            self.get_logger_adapter().error("oauth called from _make_request")  # type: ignore
            await self.oauth()
        except Exception as e:
            raise exc.UnexpectedTokenAuthError() from e

    async def get_quotes(
        self,
        instruments: List[str],
        fields: Optional[List[str]] = None,
        indicative: bool = False,
    ):
        params = {"instruments": instruments, "indicative": indicative}
        if fields is not None:
            params.update({"fields": fields})

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.Quotes,
                params=params,
                multi=len(instruments),
                capture_output=True,
            ),
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_market_hours(
        self,
        markets: Sequence[Literal['OPTION', 'EQUITY', 'FUTURE', 'BOND']],
        date: Optional[str] = None,
    ):
        params = {"markets": self._prepare_arguments_list(markets), "date": date}
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.MarketHours,
                params=params,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_expiration_chain(self, symbol: str):
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.ExpirationChain,
                params={"symbol": symbol},
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_options_chain(
        self, option_chain: Dict | OptionsChainParams, *, incl_fetch_time: bool = False
    ):
        if isinstance(option_chain, OptionsChainParams):
            params = option_chain.query_parameters
        else:
            params = option_chain

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.OptionsChain,
                params=params,
                capture_output=True,
                incl_fetch_time=incl_fetch_time,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_instruments(self, cusip_id: str):

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.GetInstruments.get_url(cusip_id),
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    # fmt: off
    async def search_instruments(
        self,
        symbol: str,
        projection: Optional[str] = None
    ):
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.SearchInstruments,
                params={"symbol": symbol, "projection": projection},
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_preferences(self):
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.UserPreference,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_transactions(
        self,
        account: str,
        transaction_type: Optional[str] = None,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        transaction_id: Optional[str] = None,
    ):
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
                " MONEY_MARKET",
                "SMA_ADJUSTMENT",
            ]:
                self.get_logger_adapter().error(
                    "The type of transaction type you specified is not valid."
                )
                raise ValueError("Bad Input")

        # if transaction_id is not none, it means we need to make a request to the get_transaction endpoint.
        if transaction_id:
            # define the endpoint
            endpoint = f"trader/v1/accounts/{account}/transactions/{transaction_id}"

            task = AsyncRequestTaskModel(  # type: ignore
                make_request=MakeRequestModel(
                    endpoint=endpoint,
                    capture_output=True,
                )
            )
            # TASK_VAR.set(task)
            # await self._get_response_from_context()

        else:
            # build the params dictionary
            params = {
                "types": transaction_type_tuple,
                "symbol": symbol,
                "startDate": start_date_tuple,
                "endDate": end_date,
            }
            self.get_logger_adapter().info(f"get transaction params: {params}")
            if account is None and self.account_number:
                account = self.account_number

            # define the endpoint
            endpoint = f"trader/v1/accounts/{account}/transactions"

            # return the response of the get request.
            task = AsyncRequestTaskModel(  # type: ignore
                make_request=MakeRequestModel(
                    endpoint=endpoint,
                    params=params,
                    capture_output=True,
                )
            )
            # TASK_VAR.set(task)
            # await self._get_response_from_context()

        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_account_number_hash_values(self):
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.AccountNumbers,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_orders_path(
        self,
        account: Optional[str] = None,
        max_results: Optional[int] = None,
        from_entered_time: Optional[str] = None,
        to_entered_time: Optional[str] = None,
        status: Optional[str] = None,
    ):
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

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=endpoint,
                params=params,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_order(self, account: str, order_id: str):
        endpoint = f"trader/v1/accounts/{account}/orders/{order_id}"
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=endpoint,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._get_response_from_context()

    async def get_accounts(
        self,
        account: Optional[str] = None,
        fields: Optional[str] = None
    ):

        endpoint = (
            "trader/v1/accounts" if not account else f"trader/v1/accounts/{account}"
        )
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=endpoint,
                params={"fields": fields} if fields else None,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()

    async def cancel_order(self, account: str, order_id: str):
        endpoint = f"trader/v1/accounts/{account}/orders/{order_id}"
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=endpoint,
                order_details=True,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()

    async def place_order(self, account: str, order: Dict):
        if isinstance(order, Order):
            order = order._grab_order()

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                method="post",
                endpoint=f"trader/v1/accounts/{account}/orders",
                mode="json",
                json=order,
                order_details=True,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()

    async def modify_order(self, account: str, order: Dict, order_id: str):
        if isinstance(order, Order):
            order = order._grab_order()

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                method="post",
                endpoint=f"trader/v1/accounts/{account}/orders/{order_id}",
                mode="json",
                json=order,
                order_details=True,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()

    async def get_movers(
        self,
        symbol_id: str,
        sort: Optional[str] = None,
        frequency: Optional[int] = None
    ):
        params = {"sort": sort, "frequency": frequency}
        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=f"marketdata/v1/movers/{symbol_id}",
                params=params,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()

    async def get_price_history(
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
    ):
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

        task = AsyncRequestTaskModel(  # type: ignore
            make_request=MakeRequestModel(
                endpoint=Endpoints.PriceHistory,
                params=params,
                capture_output=True,
            )
        )
        TASK_VAR.set(task)
        await self._ensure_session_context()
        self._ensure_headers_context()
        await self._get_response_from_context()


####################################################################################
####################################################################################
# OLD
####################################################################################


@deprecated(
    "The `_AsyncSchwabProducer` class, with the `CapacityLimiterMixin` and "
    "`LiveMixin` classes attached, has been deprecated and reimplemented. "
    "Use the `AsyncSchwabProducer` class instead."
)
class _AsyncSchwabProducer(AsyncSchwabProducerMixin, AsyncSchwabClient):

    _request_task_factory: ClassVar[Type[AsyncRequestTaskModel]] = (
        AsyncRequestTaskModel[aiohttp.ClientResponse]
    )

    def __init__(self, idx, *args, credentials=None, **kwargs) -> None:
        AsyncSchwabClient.__init__(
            self,
            idx,
            *args,
            credentials=credentials,
            **kwargs,
        )
        AsyncSchwabProducerMixin.__init__(self)

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> Self:
        self.loop = loop
        return self

    def set_request_task_factory(self, request_task_factory) -> Self:
        type(self)._request_task_factory = request_task_factory
        return self

    def get_request_task_factory(self):
        return type(self)._request_task_factory

    # async def _fetch_one(self, session: aiohttp.ClientSession, url: str):
    # async def _fetch_one(self, session: aiohttp.ClientSession, task: "AsyncRequestTaskModel"):
    async def _get_response_from_task(self):
        task = TASK_VAR.get()
        session = SESSION_VAR.get()
        headers = HEADERS_VAR.get()

        # async with self._sessionFactory() as request_session:
        start_dt = dt.datetime.now()
        perf_counter_start = perf_counter()
        url = task.summary_json.prepared_request.url
        assert isinstance(url, str)
        try:
            async with session:
                response = await session.get(
                    url=url,
                    headers=dict(list(headers.items())),
                    params=task.make_request.clean_params(),
                )
        except Exception as e:
            perf_counter_end = perf_counter()
            elapsed = dt.timedelta(seconds=perf_counter_end - perf_counter_start)
            task_info = _TaskInfo(start_dt=start_dt, elapsed=elapsed, error=e)
            TASK_INFO_VAR.set(task_info)
            await self._populate_task_summary()
            self.get_logger_adapter().error(f"{e}", exc_info=True)  # type: ignore
            return task
        else:
            perf_counter_end = perf_counter()
            elapsed = dt.timedelta(seconds=perf_counter_end - perf_counter_start)
            task_info = _TaskInfo(start_dt=start_dt, elapsed=elapsed, response=response)
            TASK_INFO_VAR.set(task_info)
            await self._populate_task_summary()
            return task
        finally:
            await self._log_fetch_times_from_task()

    async def _fetch_one(self):
        task = TASK_VAR.get()
        headers = HEADERS_VAR.get()
        session = SESSION_VAR.get()
        url = task.summary_json.prepared_request.url
        assert isinstance(url, str)
        try:
            async with session.get(url, headers=headers):
                return await self.response_handler.async_handle_task()
        except aiohttp.ClientResponseError as e:
            # Handle client errors such as 404, connection errors, etc.
            self.get_logger_adapter().error(f"Error fetching {url}: {e}")
            return e

    async def _fetch_one_in_context(self, task: "AsyncRequestTaskModel"):
        await task.get_context(TASK_VAR).run(self._fetch_one)  # type: ignore

    async def _fetch_batch(self, request_tasks: List["AsyncRequestTaskModel"]):
        tasks = [self._fetch_one_in_context(rt) for rt in request_tasks]
        return await asyncio.gather(*tasks)

    def _create_and_set_request_headers_var(
        self, mode: Optional[str] = None, is_access_token_request: bool = False
    ) -> None:  # called by _make_request, grab_access_token, exchange_code_for_token
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
        headers = super()._create_request_headers(
            mode=mode, is_access_token_request=is_access_token_request
        )
        HEADERS_VAR.set(headers)

    async def _ensure_session_context(self):
        session = await self._ensure_session()
        SESSION_VAR.set(session)

    async def _batched_make_request_from_task(
        self, request_tasks: List["AsyncRequestTaskModel"]
    ) -> List["AsyncRequestTaskModel"]:
        await self._ensure_session_context()
        session = SESSION_VAR.get()

        results = []
        async with session:

            await self.validate_tokens_from_context()
            self._create_and_set_request_headers_var()
            request_tasks = list(map(self._prepare_request_task, request_tasks))

            for i in range(0, len(request_tasks), self.batch_size):
                batch = request_tasks[i : i + self.batch_size]
                batch_results = await self._fetch_batch(batch)
                results.extend(batch_results)

            return results

    def _prepare_request_task(
        self, request_task: "AsyncRequestTaskModel"
    ) -> "AsyncRequestTaskModel":
        url = self._api_endpoint(endpoint=request_task.make_request.endpoint)

        prepared_request = requests.Request(
            method=request_task.make_request.method.upper(),
            headers=HEADERS_VAR.get(),
            url=url,
            params=request_task.make_request.params,
            data=request_task.make_request.data,
            json=request_task.make_request.jsonr,
        ).prepare()
        request_task.summary_json.set_prepared_request(prepared_request)
        assert request_task.summary_json.prepared_request.url, "url is None"
        return request_task

    async def grab_access_token_from_context(self) -> Optional[dict]:
        await self._ensure_session_context()
        session = SESSION_VAR.get()

        try:
            # Build the parameters of the request
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.state["refresh_token"],
            }
            async with await session.post(
                url=self.config["token_endpoint"],
                headers=self._create_request_headers(is_access_token_request=True),
                data=data,
            ) as response:
                if response.ok:
                    resp_data = await response.json()
                    self.get_logger_adapter().info(
                        f"Grab acces token resp: {resp_data}"
                    )
                    return self._token_save(
                        token_dict=resp_data, refresh_token_from_oauth=False
                    )
                else:
                    resp_text = await response.text()
                    if "expired" in resp_text:
                        self.get_logger_adapter().error(
                            "Failed to refresh access token. Status code: "
                            f"{response.status}, Reason: {resp_text}.  "
                            "Reauthenticating..."
                        )
                        await self.oauth()  # Re-authenticate if refresh token is also expired

        except aiohttp.ClientResponseError as e:
            self.get_logger_adapter().error(
                f"Failed to refresh access token: {e}", exc_info=True
            )
        except asyncio.TimeoutError as e:
            self.get_logger_adapter().error(
                f"Failed to refresh access token: {e}", exc_info=True
            )

    async def validate_tokens_from_context(self) -> bool:
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
                await self.oauth()
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
                await self.grab_access_token()
            return True
        else:
            # token expire times are not in self.state
            return await self.oauth()

    # async def oauth(self) -> bool:
    #     """Runs the oAuth process for the Broker API."""
    #     # called by login(no credentials file) and validate_tokens (a token is expired) or _make_request if response not OK
    #     # Create the Auth URL.
    #     url = f"{self.config['auth_endpoint']}client_id={self.client_id}&redirect_uri={self.redirect_uri}"

    #     print(
    #         f"Please go to URL provided to authorize your account(idx={self.idx}): {url}"  # type: ignore
    #     )
    #     # Paste it back and send it to exchange_code_for_token.
    #     redirect_url = input("Paste the full URL redirect here: ")

    #     self.get_logger_adapter().debug(f"redirect_url: {redirect_url}")
    #     self.code = self._extract_redirect_code(redirect_url)
    #     self.get_logger_adapter().debug(f"self.code: {self.code}")

    #     # Exchange the Auth Code for an Access Token.
    #     return await self.exchange_code_for_token()

    @staticmethod
    def _get_task_sendproto(task=None):
        task = task or TASK_VAR.get()
        return task.make_request.send_protocol

    @staticmethod
    def _get_task_data(task=None) -> str:
        task = task or TASK_VAR.get()
        if task.summary_json.response_data is not None:
            data = json.dumps(task.summary_json.response_data)
        elif task.summary_json.error is not None:
            data = str(task.summary_json.error)
        else:
            data = task.summary_json.model_dump_json()
        return data

    async def _execute_sendproto(self):
        task = TASK_VAR.get()
        if (sendproto := self._get_task_sendproto(task)) is not None:

            data = self._get_task_data(task)

            from ...requestlib.make import (
                HttpSendProtocol,
                RedisSendProtocol,
                SocketSendProtocol,
                KafkaSendProtocol,
            )

            if isinstance(sendproto, RedisSendProtocol):
                async with self.get_redis() as conn:
                    await conn.rpush(sendproto.key, data)

            elif isinstance(sendproto, HttpSendProtocol):
                import requests

                url = sendproto.url
                requests.post(url, json={"data": data})

            elif isinstance(sendproto, SocketSendProtocol):
                import socket

                host = sendproto.host
                port = sendproto.port
                data_encoded = json.dumps(data).encode()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                    client_socket.connect((host, port))
                    message_length = f"{len(data_encoded):<10}"
                    client_socket.sendall(message_length.encode() + data_encoded)

            else:
                self.get_logger_adapter().error(
                    f"Unrecognized send protocol {type(sendproto).__name__}"
                )

    async def _handle_auth_error(
        self,
        response: "requests.Response",
        request: "requests.PreparedRequest",
        summary_json: "_SummaryModel",
    ) -> None:
        resp_text = response.text
        self.get_logger_adapter().error(f"make_requests error = {resp_text}")  # type: ignore
        try:
            self.get_logger_adapter().error("oauth called from _make_request")  # type: ignore
            await self.oauth()
        except Exception as e:
            raise exc.UnexpectedTokenAuthError() from e
