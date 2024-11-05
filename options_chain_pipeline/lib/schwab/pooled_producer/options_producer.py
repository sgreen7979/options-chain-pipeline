#!/usr/bin/env python3
import argparse
import asyncio
from collections import defaultdict
from contextvars import copy_context
import datetime as dt
import itertools
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
    Optional,
    Union,
)

from kafka.errors import KafkaError

from daily.fundamental.loader import FundamentalsLoader
from daily.symbols import get_options_universe, body_too_big
from daily.utils.logging import add_new_fh, get_logger, ClassNameLoggerMixin
from daily.utils.profiling import init_profiler

# import daily.utils.async_utils.event_loop as looppolicy

from ..api import Endpoints
from ..capacity import _wrap_capacity_command_callback as capacity_cmd_cb
from ..client_pool.asynchronous import AsyncClientPool
from ..market_hours.xpath import XPaths
from ..option_chain import (
    OptionsChainParams,
    OptionsChainResponseDataValidator,
)
from ..producer.services.kafka.utils import extract_producer_configs
from ..requestlib.adapters import BodyTooBigRequest, OptionsChainRequest

# from ..requestlib.ctx import TASK_VAR
from ..requestlib.ctx import get_and_set_async_request_task_var
from ..requestlib.instrument import LoggingInstrument, register_instrument
from ..requestlib.make import MakeRequestModel
from ..requestlib.task import AsyncRequestTaskModel
from .base_producer import BaseProducer, KAFKA_CONFIG

if TYPE_CHECKING:
    from ..producer.client.asynchronous import AsyncSchwabProducer

LOG_LEVEL = "DEBUG"
logger = get_logger(__name__, LOG_LEVEL, ch=True, ch_level="INFO")
TASK_VAR = get_and_set_async_request_task_var()


class OptionsChainProducer(BaseProducer):
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
    DEFAULT_KAFKA_TOPIC_PREFIX: str = "option_chain_topic"

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
                    Callable[[asyncio.AbstractEventLoop, Dict], Optional[Any]],
                ] = defaultdict(lambda: self.handle_task_exception)

            def handle_task_exception(
                self, loop: asyncio.AbstractEventLoop, context: Dict
            ):
                exception = context.get("exception")
                message = context.get("message", "")

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
        kafka_topic_prefix: Optional[str] = None,
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
            # topic_date=kafka_topic_date,
            # kafka_config=kafka_config,
        )
        self._loop_exception_handler = (
            type(self).LoopConfig.ExceptionHandler().handle_task_exception
        )
        self._graceful_shutdown_handler = (
            type(self).LoopConfig.GracefulShutdownHandler().graceful_shutdown
        )
        self.validator = OptionsChainResponseDataValidator()
        self.set_queue_capacity(queue_capacity)

        if fundamentals_loader is not None:
            self.fundamentals = fundamentals_loader
        else:
            if TYPE_CHECKING:
                assert isinstance(self.clients, AsyncClientPool)
            self.fundamentals = FundamentalsLoader(self._symbols, self.clients)

        self._set_hours()
        self._topic_prefix = kafka_topic_prefix or self.DEFAULT_KAFKA_TOPIC_PREFIX
        self._topic_date = kafka_topic_date or dt.date.today()
        self._kafka_topic = (
            f"{self._topic_prefix}_{self._topic_date.strftime('%Y%m%d')}"
        )
        logger.info(f"regstart: {self.regstart.isoformat(' ', 'minutes')}")
        logger.info(f"regend: {self.regend.isoformat(' ', 'minutes')}")
        logger.info(f"queue capacity: {self.queue_capacity}")
        logger.info("Initialized")

    def set_up_kafka_service(self):

        sink_producer_configs = extract_producer_configs(KAFKA_CONFIG)
        self.sink_producer = self._kafka_service.initialize_producer(
            **sink_producer_configs
        )

        self._kafka_service.new_topic(self._kafka_topic, num_partitions=16)

        topics = [f"async-schwab-producer-{idx}" for idx in self.clients.get_idxs()]
        self._kafka_capacity_service = self._kafka_service.initialize_scoped_service(
            *topics, handler_callbacks=[capacity_cmd_cb(self)]
        )
        self._kafka_capacity_service.start(timeout_ms=30_000, to_thread=True)

    async def cleanup(self):
        await self.fundamentals._clear_cache()
        self._kafka_service.stop()
        await super().cleanup()

    async def add_symbol(self, symbol: str) -> None:
        super().add_symbol(symbol)
        await self.fundamentals.fetch([symbol])

    async def add_symbols(self, symbols: List[str]) -> None:
        super().add_symbols(symbols)
        await self.fundamentals.fetch(symbols)

    def _handle_make_request_exception(self):
        task = TASK_VAR.get()
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
                        self._kafka_sink, symbol, data
                    )
                    task.sink()
        else:
            self._handle_make_request_exception()

        task.complete()

    def _kafka_sink(self, symbol: str, data: Dict, flush: bool = False):
        def _on_send_success(record_metadata):
            self.get_logger().debug(
                f"Sent {symbol} message to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
            )

        def _on_send_error(excp):
            self.get_logger().error(f'Failed to send message {symbol}', exc_info=excp)

        try:
            self._kafka_service.publish(
                self._kafka_topic,
                producer=self.sink_producer,
                key=symbol.encode(),
                value=data,
                callbacks=[_on_send_success],
                errbacks=[_on_send_error],
                flush=flush,
            )

        except KafkaError as e:
            self.get_logger().error(f"Failed to send data to Kafka for {symbol}: {e}")

    async def _stage_requests_loop(self, start_event: asyncio.Event):
        symbols = iter(self.symbols)
        self._round_trip = 1
        self.get_logger().info(f"Starting round trip {self._round_trip}")
        loop_count = 0
        loop_counter = itertools.count().__next__
        filled = [False for _ in self.clients]
        _cut = []

        while True:
            c = self.clients.next_client()
            loop_count += loop_counter()
            if (queue_capacity := await c.get_available_queue_capacity()) > 0:  # type: ignore
                initial_queue_capacity = queue_capacity
                while queue_capacity:
                    if self.graceful_exit_event.is_set():
                        raise InterruptedError("Graceful exit event set")
                    elif self.past_regend:
                        self.get_logger().info("Options markets are closed")
                        await self.stop(hard_stop=True)
                        return
                    else:
                        try:
                            if _cut:
                                make_req = _cut.pop(0)
                                symbol = None
                            else:
                                symbol = next(symbols)
                        except StopIteration:
                            self._round_trip += 1
                            self.get_logger().info(
                                f"Starting round trip {self._round_trip}"
                            )
                            symbols = iter(self.symbols)
                            symbol = next(symbols)
                        finally:

                            if symbol is None:
                                assert make_req
                                task = await self.clients.enqueue_make_request(
                                    make_req, client=c
                                )

                            elif symbol in body_too_big:
                                make_reqs = BodyTooBigRequest(
                                    symbol=symbol, incl_fetch_time=True
                                ).list_tasks
                                _cut.extend(make_reqs[1:])
                                task = await self.clients.enqueue_make_request(
                                    make_reqs[0], client=c
                                )
                            else:
                                task = await self.clients.enqueue_make_request(  # noqa: F841
                                    type(self).OptionsChainRequestFactory(
                                        symbol=symbol,
                                        incl_fetch_time=True,
                                    ),
                                    client=c,
                                )
                            self._total_requests += 1
                            queue_capacity -= 1

                c.get_logger_adapter().info(f"Staged {initial_queue_capacity} tasks")

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
            del start_event

            self.clients.start(
                callback=self._handle_completed_task,
                endpoint=Endpoints.OptionsChain,
            )

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


def _arg_parser():
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

    return parser


def usage():
    _arg_parser().print_usage()


def parse_args():
    return _arg_parser().parse_args()


async def main(loop):

    from ..client.functions import get_all_clients
    from ..producer.client.asynchronous import AsyncSchwabProducer

    clients = get_all_clients(
        client_factory=AsyncSchwabProducer,
        login=True,
    )

    AsyncRequestTaskModel.attach_instrument(LoggingInstrument())
    # register_instrument(LoggingInstrument())

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
