#!/usr/bin/env python3
import argparse
import asyncio
import atexit
from collections import defaultdict
from contextvars import copy_context, Context
import datetime as dt
import itertools
import json
import os
import tracemalloc
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    DefaultDict,
    Dict,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
    Unpack,
    cast,
)

import psutil
from pydantic import Field

from daily.fundamental.loader import FundamentalsLoader
from daily.symbols import get_options_universe, body_too_big
from daily.utils.logging import add_new_fh, get_logger, ClassNameLoggerMixin
from daily.utils.profiling import init_profiler

# import daily.utils.async_utils.event_loop as looppolicy

from ..api import Endpoints, Endpoint
from ..market_hours.xpath import XPaths
from ..option_chain import (
    OptionsChainParams,
    OptionsChainResponseDataValidator,
)
from ..producer.services.kafka import KafkaService, KafkaHandler
from ..requestlib.ctx import TASK_VAR
from ..requestlib.instrument import LoggingInstrument
from ..requestlib.make import MakeRequestModel
from ..requestlib.task import AsyncRequestTaskModel
from .base_producer import BaseKafkaProducer

if TYPE_CHECKING:
    from kafka import KafkaProducer
    from ..producer.client.asynchronous import AsyncSchwabProducer
    from kafka.consumer.fetcher import ConsumerRecord

# Logging setup
LOG_LEVEL = "DEBUG"
logger = get_logger(__name__, LOG_LEVEL, ch=True, ch_level="INFO")


class CapacityCommand(TypedDict, total=True):
    action: Literal["reserve", "unreserve"]
    capacity: int
    pid: Optional[int]
    confirm_topic: str
    client_idx: int


def new_capacity_command(**params: Unpack[CapacityCommand]):
    if params["pid"] is None:
        params["pid"] = os.getpid()
    assert params["action"] in ("reserve", "unreserve")
    assert params["capacity"] > 0
    assert params["confirm_topic"]
    assert params["client_idx"]
    return params


def validate_capacity_command(command: dict) -> CapacityCommand:
    assert command["action"] in ("reserve", "unreserve")
    assert command["capacity"] > 0
    assert command["confirm_topic"]
    assert command["client_idx"]
    return cast(CapacityCommand, command)


async def capacity_command_callback(
    message: "ConsumerRecord", runner: "OptionsChainProducer"
):
    if isinstance(message.value, bytes):
        command = json.loads(message.value.decode())
    elif isinstance(message.value, str):
        command = json.loads(message.value)
    elif isinstance(message.value, dict):
        command = message.value
    else:
        raise TypeError(f"Message value of unexpected type {message.value}")
    command = validate_capacity_command(command)
    client_idx = command["client_idx"]
    client = runner.clients.get_client_by_idx(client_idx)

    current_reserved_capacity = await client.reserved

    if command["action"] == "reserve":
        capacity_to_reserve = command["capacity"]
        updated_reserved_capacity = current_reserved_capacity + capacity_to_reserve

    else:
        capacity_to_unreserve = command["capacity"]
        updated_reserved_capacity = current_reserved_capacity - capacity_to_unreserve

    async with client.get_redis() as conn:
        await conn.set(client.redis_keys.reserved, updated_reserved_capacity)

    return command


def _wrap_capacity_command_callback(runner: "OptionsChainProducer"):

    callback = capacity_command_callback

    async def wrapper(message: "ConsumerRecord"):
        command = await callback(message, runner)

        if command["action"] == "reserve":
            confirm_msg = "Confirming capacity reservation of {}"
        else:
            confirm_msg = "Confirming release of capacity reservation of {}"

        topic = command["confirm_topic"]
        capacity = command["capacity"]
        confirm_msg = confirm_msg.format(capacity).encode()
        runner._kafka_service.publish(topic, confirm_msg).flush()

    return wrapper


class OptionsChainRequest(MakeRequestModel):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    symbol: str = Field()
    endpoint: Union[str, Endpoint] = Endpoints.OptionsChain

    def model_post_init(self, __context: Any = None):
        self.params: Dict = OptionsChainParams(
            symbol=self.symbol,
            strategy=OptionsChainParams.Strategy.ANALYTICAL,
            include_quotes=True,
        ).query_parameters


OptionsChainRequest = MakeRequestModel.register(OptionsChainRequest)


class BodyTooBigRequest(MakeRequestModel):
    """
    This class represents a collection of requests related to particular
    symbols whose options chains are so large Schwab's server complains
    they're too big to send in oneshot.  We instead break up the request
    by `expMonth` (and, for some, `contractType`)

    2024-10-13 21:11:28,809 ERROR daily.utils.logging Request error {"fault":{"faultstring":"Body buffer overflow","detail":{"errorcode":"protocol.http.TooBigBody"}},"params":{"symbol": "$NDX", "contractType": "ALL", "includeUnderlyingQuote": "True", "strategy": "ANALYTICAL", "range": "ALL", "expMonth": "ALL", "optionType": "ALL"},"endpoint":"/marketdata/v1/chains"}
    """

    symbol: str = Field()
    endpoint: Union[str, Endpoint] = Endpoints.OptionsChain
    list_params: List[Dict] = Field(default_factory=list)
    list_tasks: List["MakeRequestModel"] = Field(default_factory=list)

    def model_post_init(self, __context: Any = None):
        self.list_params = []
        self.list_tasks = []
        if self.symbol == "$NDX":
            self.list_params.extend(
                [
                    OptionsChainParams(
                        symbol=self.symbol,
                        strategy=OptionsChainParams.Strategy.ANALYTICAL,
                        include_quotes=True,
                        exp_month=month.value,
                        contract_type="CALL",
                    ).query_parameters
                    for month in OptionsChainParams.Months
                ]
            )
            self.list_params.extend(
                [
                    OptionsChainParams(
                        symbol=self.symbol,
                        strategy=OptionsChainParams.Strategy.ANALYTICAL,
                        include_quotes=True,
                        exp_month=month.value,
                        contract_type="PUT",
                    ).query_parameters
                    for month in OptionsChainParams.Months
                ]
            )
        else:
            self.list_params.extend(
                [
                    OptionsChainParams(
                        symbol=self.symbol,
                        strategy=OptionsChainParams.Strategy.ANALYTICAL,
                        include_quotes=True,
                        exp_month=month.value,
                    ).query_parameters
                    for month in OptionsChainParams.Months
                ]
            )
        self.list_tasks.extend(
            [
                MakeRequestModel(
                    endpoint=self.endpoint,
                    params=p,
                    incl_fetch_time=self.incl_fetch_time,
                    capture_output=self.capture_output,
                    incl_response=self.incl_response,
                    send_protocol_dict=self.send_protocol_dict,
                )
                for p in self.list_params
            ]
        )


BodyTooBigRequest = MakeRequestModel.register(BodyTooBigRequest)


class OptionsChainProducer(BaseKafkaProducer):
    OptionsChainParams = OptionsChainParams
    OptionsChainRequestFactory: ClassVar[Callable[..., MakeRequestModel]] = (
        OptionsChainRequest
    )
    MARKET = XPaths.option.name
    REGSTART_XPATH = XPaths.option.EQO.regstart
    REGEND_XPATH = XPaths.option.IND.regend
    LOGGING_FILE_HANDLER_KWARGS: dict[str, Any] = dict(
        fh_level="DEBUG",
        fh_fmt="%(asctime)s %(levelname)s %(name)s %(pathname)s %(lineno)d %(message)s",
        fh_type="RotatingFileHandler",
        fh_type_kwargs={"maxBytes": 1_048_576, "backupCount": 500},  # 1MB
    )

    class MarketHoursConfig:
        MARKET = XPaths.option.name
        REGSTART_XPATH = XPaths.option.EQO.regstart
        REGEND_XPATH = XPaths.option.IND.regend

    class LoggingConfig:
        LOGGING_FILE_HANDLER_KWARGS: dict[str, Any] = dict(
            fh_level="DEBUG",
            fh_fmt="%(asctime)s %(levelname)s %(name)s %(pathname)s %(lineno)d %(message)s",
            fh_type="RotatingFileHandler",
            fh_type_kwargs={"maxBytes": 1_048_576, "backupCount": 500},  # 1MB
        )

    class LoopConfig:
        class ExceptionHandler(ClassNameLoggerMixin):

            def __init__(self):
                self._registered_exception_handlers: DefaultDict[
                    Union[Exception, BaseException],
                    Callable[
                        [asyncio.AbstractEventLoop, Context | Dict], Optional[Any]
                    ],
                ] = defaultdict(lambda: self.handle_task_exception)

            def handle_task_exception(self, loop: asyncio.AbstractEventLoop, context):
                # Extract details of the exception from context
                exception = context.get("exception")
                message = context.get("message", "")

                # Get more context details like task details or where it occurred
                task = context.get("task")
                if task:
                    task_name = task.get_name()
                else:
                    task_name = "Unknown Task"

                logger = type(self).get_logger()

                logger.error(f"Error: {task_name=}")

                if exception:
                    logger.error(f"Caught exception in {task_name}: {exception=}")
                else:
                    logger.error(f"Caught error message in {task_name}: {message=}")

                # Log full context for better debugging
                logger.error(f"Full context: {context=}")

        class GracefulShutdownHandler(ClassNameLoggerMixin):
            async def graceful_shutdown(
                self,
                loop: asyncio.AbstractEventLoop,
                runner: "OptionsChainProducer",
                signal=None,
            ):
                runner.graceful_exit_event.set()
                runner.clients.stop(hard_stop=True)
                await runner.stop()

                type(self).get_logger().info(
                    f"Received exit signal. Shutting down gracefully... ({loop!r})"
                )
                tasks = [
                    task
                    for task in asyncio.all_tasks(loop)
                    if task is not asyncio.current_task(loop)
                ]

                # Log pending tasks
                type(self).get_logger().info(
                    f"Cancelling {len(tasks)} outstanding tasks... ({loop!r})"
                )

                for task in tasks:
                    task.cancel()

                await asyncio.gather(*tasks, return_exceptions=True)

                if not runner.end_event.is_set():
                    runner.end_event.set()

                loop.stop()

    @classmethod
    def make_task_factory(cls, symbol: str):
        return MakeRequestModel(
            endpoint=Endpoints.OptionsChain,
            params=OptionsChainParams(
                symbol=symbol,
                strategy=OptionsChainParams.Strategy.ANALYTICAL,
                include_quotes=True,
            ).query_parameters,
        )

    def __init__(
        self,
        symbols: Union[List[str], Callable[[], List[str]]],
        *,
        clients: Optional[List["AsyncSchwabProducer"]] = None,
        test: bool = False,
        test_time_minutes: float = 20.0,
        sink: bool = True,
        regstart: Optional[dt.datetime] = None,
        regend: Optional[dt.datetime] = None,
        queue_capacity: int = 500,
        kafka_topic_date: Optional[dt.date] = None,
        kafka_config: Optional[Dict] = None,
        fundamentals_loader: Optional["FundamentalsLoader"] = None,
    ) -> None:
        super().__init__(
            symbols,
            clients=clients,
            test=test,
            test_time_minutes=test_time_minutes,
            sink=sink,
            regstart=regstart,
            regend=regend,
            topic_date=kafka_topic_date,
            kafka_config=kafka_config,
        )
        self._loop_exception_handler = (
            type(self).LoopConfig.ExceptionHandler().handle_task_exception
        )
        self._graceful_shutdown_handler = (
            type(self).LoopConfig.GracefulShutdownHandler().graceful_shutdown
        )
        # self.request_params = chain_req_params.query_parameters
        self.validator = OptionsChainResponseDataValidator()
        self.set_queue_capacity(queue_capacity)

        if fundamentals_loader is not None:
            self.fundamentals = fundamentals_loader
        else:
            self.fundamentals = FundamentalsLoader(self._symbols, self.clients)

        self._set_hours()
        logger.info(f"regstart: {self.regstart.isoformat(' ', 'minutes')}")
        logger.info(f"regend: {self.regend.isoformat(' ', 'minutes')}")
        logger.info(f"queue capacity: {self.queue_capacity}")
        logger.info("Initialized")

    def _start_kafka_service(self):
        self._kafka_service = KafkaService()
        topics = [f"async-schwab-producer-{idx}" for idx in self.clients.get_idxs()]
        consumer = self._kafka_service.initialize_consumer(*topics)
        self._kafka_service.initialize_producer()
        handler = KafkaHandler(
            *topics, callbacks=[_wrap_capacity_command_callback(self)]
        )
        self._kafka_service.add_handler(handler)
        self._kafka_service.listen(consumer)

    def log_performance_metrics(self):
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)
        self.get_logger().info(f"Memory Usage: {mem.percent}% | CPU Usage: {cpu}%")

    def set_queue_capacity(self, n: int):
        assert n > 0
        self._queue_capacity = n
        for client in self.clients:
            client.set_queue_capacity(self.queue_capacity)
        return self

    @property
    def queue_capacity(self):
        return self._queue_capacity

    async def cleanup(self):
        await self.fundamentals._clear_cache()
        if not self._kafka_service._stop_event.is_set():
            self._kafka_service._stop_event.set()
        if (
            self._kafka_service._is_multiprocess
            and not self._kafka_service.mpevent.is_set()
        ):
            self._kafka_service.mpevent.set()
        await super().cleanup()

    async def add_symbol(self, symbol: str) -> None:
        super().add_symbol(symbol)
        await self.fundamentals.fetch([symbol])

    async def add_symbols(self, symbols: List[str]) -> None:
        super().add_symbols(symbols)
        await self.fundamentals.fetch(symbols)

    def _handle_make_request_exception(self):
        task = TASK_VAR.get()
        self.get_logger().error(
            f"Request error {task.summary_json.error}", exc_info=True, stacklevel=2  # type: ignore
        )
        if task.summary_json.response is not None:
            if task.summary_json.response.reason.lower() == "bad request":  # type: ignore
                symbol = task.make_request.params["symbol"]  # type: ignore
                self.remove_symbol(symbol)  # type: ignore

    async def _handle_completed_task(self):
        task = TASK_VAR.get()
        assert task.summary_json is not None
        if task.summary_json.error is None:
            assert (data := task.summary_json.response_data) is not None
            if (
                symbol := data.get("symbol", None)
            ) is not None and self.validator.validate(data):
                data["dividendAmount"] = await self.fundamentals.get(symbol)
                if task.make_request.incl_fetch_time:
                    assert isinstance(task.summary_json.fetchTime, str)
                    data["fetchTime"] = task.summary_json.fetchTime
                if "response" in data:
                    del data["response"]
                if self._sink:
                    asyncio.get_running_loop().call_soon_threadsafe(
                        self._to_kafka, symbol, data
                    )
        else:
            self._handle_make_request_exception()

        task.complete()

    async def _stage_requests_loop(self, start_event: asyncio.Event):
        symbols = iter(self.symbols)
        self._round_trip = 1
        self.get_logger().info(f"Starting round trip {self._round_trip}")
        loop_count = 0
        count = itertools.count().__next__
        filled = [False for _ in self.clients]
        while True:
            c = self.clients.next_client()
            loop_count += count()
            if (queue_capacity := await c.get_available_queue_capacity()) > 0:
                for _ in range(queue_capacity):
                    if self.graceful_exit_event.is_set():
                        raise InterruptedError("Graceful exit event set")
                    elif self.past_regend:
                        self.get_logger().info("Options markets are closed")
                        await self.stop(hard_stop=True)
                        return
                    else:
                        try:
                            symbol = next(symbols)
                        except StopIteration:
                            self._round_trip += 1
                            self.get_logger().info(
                                f"Starting round trip {self._round_trip}"
                            )
                            symbols = iter(self.symbols)
                            symbol = next(symbols)
                        finally:

                            if symbol in body_too_big:
                                make_reqs = BodyTooBigRequest(
                                    symbol=symbol, incl_fetch_time=True
                                ).list_tasks
                                for make_req in iter(make_reqs):
                                    task = await self.clients.enqueue_make_request(
                                        make_req, client=c
                                    )
                                    # assert task.make_request.params is not None
                                    # self.get_logger().debug(
                                    #     f"Task created {task.uuid} for {symbol} expMont={task.make_request.params['expMonth']} with client {task.client_idx}"
                                    # )

                            else:
                                task = await self.clients.enqueue_make_request(  # noqa: F841
                                    type(self).OptionsChainRequestFactory(
                                        symbol=symbol,
                                        incl_fetch_time=True,
                                    ),
                                    client=c,
                                )
                                # self.get_logger().debug(
                                #     f"Task created {task.uuid} for {symbol} with client {task.client_idx}"  # type: ignore
                                # )
                            self._total_requests += 1

                c.get_logger_adapter().info(f"Staged {queue_capacity} tasks")

            elif filled is not None:
                filled[self.clients.index(c)] = True
                if all(filled):
                    start_event.set()
                    filled = None

            self.log_performance_metrics()
            if not loop_count % len(self.clients):
                sleep_time = 10.0
            else:
                sleep_time = 1.0
            await asyncio.sleep(sleep_time)

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.clients.set_loop(loop)

    async def run(self) -> None:
        global _PROPOGATE_ERROR

        add_new_fh(self.get_logger(), **self.LOGGING_FILE_HANDLER_KWARGS)

        await self.fundamentals.await_init_tasks()
        await self.fundamentals.fetch(self._symbols)
        await self.clients.clear_queues()

        try:
            await self._wait_until_regstart()

            start_event = asyncio.Event()

            self.loop.create_task(self._stage_requests_loop(start_event))

            await start_event.wait()
            self.get_logger().info("Queues have been pre-filled, starting the clients")

            self.clients.start(
                callback=self._handle_completed_task,
                endpoint=Endpoints.OptionsChain,
            )

            self._start_kafka_service()

            await self.end_event.wait()

        except InterruptedError as e:
            self.get_logger().error("InterruptedError: attempting graceful shutdown")
            if _PROPOGATE_ERROR is None:
                _PROPOGATE_ERROR = e
            return
        except SystemExit as e:
            self.get_logger().error("SystemExit: attempting graceful shutdown")
            if _PROPOGATE_ERROR is None:
                _PROPOGATE_ERROR = e
            return
        except KeyboardInterrupt as e:
            self.get_logger().error("KBI: attempting graceful shutdown")
            if _PROPOGATE_ERROR is None:
                _PROPOGATE_ERROR = e
            return


def parse_hours(date_string):
    try:
        return dt.datetime.fromisoformat(date_string)
    except ValueError as e:
        try:
            return dt.datetime.fromtimestamp(float(date_string))
        except TypeError as ee:
            raise ee from e


def parse_args():
    parser = argparse.ArgumentParser()
    aa = parser.add_argument
    aa("--test", dest="test", action="store_true", default=False)
    aa("--test-time", dest="test_time", type=float, default=20.0)
    aa("--no-sink", dest="sink", action="store_false", default=True)
    aa("--regstart", dest="regstart", type=parse_hours, default=None)
    aa("--regend", dest="regend", type=parse_hours, default=None)
    aa("--queue-capacity", dest="queue_capacity", type=int, default=500)
    aa("--sched-capacity", dest="sched_capacity", type=int, default=120)
    aa("--prep-capacity", dest="prep_capacity", type=int, default=120)
    aa("--instruments", nargs="?", type=str, default=None)
    aa(
        "--kafka-topic-date",
        dest="kafka_topic_date",
        type=dt.date.fromisoformat,
        default=None,
    )
    aa(
        "--profile",
        action="store_true",
        default=False,
        help="turn code profiling on",
    )
    aa(
        "--tracemalloc",
        action="store_true",
        default=False,
        help="turn tracemalloc on",
    )
    aa(
        "--tracemalloc-nframes",
        dest="tracemalloc_nframes",
        type=int,
        default=5,
        help="set tracemalloc nframes",
    )
    aa(
        "--asyncio-debug",
        dest="asyncio_debug",
        action="store_true",
        default=False,
        help="enable asyncio debugging",
    )

    return parser.parse_args()


async def main(loop):

    from ..client.functions import get_all_clients_async
    from ..producer.client.asynchronous import AsyncSchwabProducer

    clients = await get_all_clients_async(
        client_factory=AsyncSchwabProducer,
        login=True,
    )

    AsyncRequestTaskModel.attach_instrument(LoggingInstrument())

    global RUNNER
    RUNNER = OptionsChainProducer(
        get_options_universe,
        clients=clients,
        queue_capacity=args.queue_capacity,
        test=args.test,
        test_time_minutes=args.test_time,
        sink=args.sink,
        regstart=args.regstart,
        regend=args.regend,
        kafka_topic_date=args.kafka_topic_date,
    )
    RUNNER.set_loop(loop)

    if args.profile:
        init_profiler(__name__, logger=RUNNER.get_logger())

    loop.set_exception_handler(RUNNER._loop_exception_handler)
    try:
        await RUNNER.run()
    except KeyboardInterrupt:
        await RUNNER._graceful_shutdown_handler(loop, RUNNER, copy_context())
        raise

    except Exception as e:
        RUNNER.get_logger().error(f"Failed to run until completion {e}", exc_info=True)
        raise
    finally:
        await RUNNER.stop()
        loop.stop()


if __name__ == "__main__":
    args = parse_args()
    loop = asyncio.get_event_loop()

    if args.tracemalloc:
        tracemalloc.start(args.tracemalloc_nframes)

    global _PROPOGATE_ERROR
    _PROPOGATE_ERROR = None

    if args.asyncio_debug:
        os.environ['PYTHONASYNCIODEBUG'] = '1'

    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt as e:
        if _PROPOGATE_ERROR is None:
            _PROPOGATE_ERROR = e
        asyncio.run(RUNNER._graceful_shutdown_handler(loop, RUNNER))
    finally:
        loop.close()
