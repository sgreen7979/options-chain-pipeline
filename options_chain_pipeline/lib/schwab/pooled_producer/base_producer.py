#!/usr/bin/env python3
from abc import abstractmethod
import asyncio
import asyncio.base_futures
import asyncio.futures
import asyncio.tasks
import atexit
from copy import deepcopy
import datetime as dt
import gzip
import json
import sys
import threading
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import psutil

from daily.market_hrs import functions as mh
from daily.utils.logging import ClassNameLoggerMixin
from daily.utils.requests_dataclasses import RequestOutcome, RequestStatistics

from ..client_pool.base import ClientPool

# from ..client_pool.synchronous import SyncClientPool
# from ..client_pool.asynchronous import AsyncClientPool
from ..market_hours import XPaths, MarketsType, MarketType
from ..producer.services.kafka import KafkaService, KafkaHandler
from ..producer.services.kafka.utils import get_broker_api_version
from ..capacity import _wrap_capacity_command_callback as capcity_cmd_cb

if TYPE_CHECKING:
    import requests
    from ..producer.client.asynchronous import AsyncSchwabProducer
    from ..producer.client.synchronous import SyncSchwabProducer
    from ..client_pool.asynchronous import AsyncClientPool
    from ..client_pool.synchronous import SyncClientPool

# _ProducerType = TypeVar("_ProducerType", "AsyncSchwabProducer", "SyncSchwabProducer")
_ProducerType = TypeVar("_ProducerType")
_ProducerPoolType = TypeVar(
    # "_ProducerPoolType", bound=Union["SyncClientPool", "AsyncClientPool"]
    "_ProducerPoolType",
    bound=ClientPool,
)


# Configuration
API_VERSION = get_broker_api_version()
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_PREFIX = "option_chain_topic"
KAFKA_NUM_PARTITIONS = 16
KAFKA_REPLICATION_FACTOR = 1
KAFKA_TOPIC_CONFIG = {
    "topic_prefix": KAFKA_TOPIC_PREFIX,
    "num_partitions": KAFKA_NUM_PARTITIONS,
    "replication_factor": KAFKA_REPLICATION_FACTOR,
}
KAFKA_PRODUCER_CONFIG = {
    "value_serializer": lambda v: gzip.compress(json.dumps(v).encode('utf-8')),
    "max_request_size": 10_485_760,  # 10MB
}
KAFKA_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "api_version": API_VERSION,
    "topic_prefix": KAFKA_TOPIC_PREFIX,
    "num_partitions": KAFKA_NUM_PARTITIONS,
    "replication_factor": KAFKA_REPLICATION_FACTOR,
    "max_request_size": 10_485_760,  # 10MB
    "compression_type": 'gzip',
    "value_serializer": lambda v: gzip.compress(json.dumps(v).encode('utf-8')),
    "request_timeout_ms": 600_000,
    "max_block_ms": 1_200_000,
    "retries": 5,
}


class BaseProducer(ClassNameLoggerMixin):

    MARKET: ClassVar[MarketType] = "EQUITY"
    REGSTART_XPATH: ClassVar[str] = XPaths.equity.EQ.regstart
    REGEND_XPATH: ClassVar[str] = XPaths.equity.EQ.regend
    LEVEL = "INFO"
    CH = True
    CACHED = True

    def __init__(
        self,
        symbols: Union[List[str], Callable[[], List[str]]],
        *,
        clients: Optional[List["AsyncSchwabProducer"]] = None,
        # clients: Optional[
        #     List["AsyncSchwabProducer"] | List["SyncSchwabProducer"]
        # ] = None,
        test: bool = False,
        test_time_minutes: float = 20.0,
        sink: bool = True,
        regstart: Optional[dt.datetime] = None,
        regend: Optional[dt.datetime] = None,
    ) -> None:
        from ..client_pool.asynchronous import AsyncClientPool

        self.clients = AsyncClientPool(clients=clients).set_runner(self)
        self._symbols: List[str] = (
            symbols() if isinstance(symbols, Callable) else symbols
        )
        self._test = test
        self._test_time_minutes = test_time_minutes
        self._sink = sink
        self.graceful_exit_event = threading.Event()

        # For testing purposes, it is safer getting and setting our hours after
        # the upfront work in fetching dividendAmounts
        self.regstart: dt.datetime
        self.regend: dt.datetime
        self._regstart = regstart
        self._regend = regend

        self._removed_symbols: List[str] = []
        self._running = self._ran = False
        self._round_trip = self._batch_num = 0
        self._request_statistics = RequestStatistics()
        self._total_requests = 0
        self.end_event = asyncio.Event()

        self._get_kafka_service()

        self.get_logger().info(f"Number of clients {len(self.clients)}")
        self.get_logger().info(
            f"Total rolling request capacity {self.clients._rolling_capacity()}"
        )
        self.get_logger().info(f"Number of symbols {len(self._symbols)}")
        self.get_logger().info(
            f"Estimated round trip time {self.round_trip_time} minutes"
        )

    # @overload
    # def _create_client_pool(
    #     self, clients: Optional[List["AsyncSchwabProducer"]]
    # ) -> "AsyncClientPool": ...

    # @overload
    # def _create_client_pool(
    #     self, clients: Optional[List["SyncSchwabProducer"]]
    # ) -> "SyncClientPool": ...

    # @abstractmethod
    # def _create_client_pool(
    #     self,
    #     clients: Optional[
    #         Union[List["SyncSchwabProducer"], List["AsyncSchwabProducer"]]
    #     ],
    # ) -> Union["SyncClientPool", "AsyncClientPool"]: ...

    def _get_kafka_service(self):
        self._kafka_service = KafkaService()
        self.set_up_kafka_service()

    def set_up_kafka_service(self, *args, **kwargs):
        """
        Override this function to specify components
        of the kafka service you would like to include
        """
        pass

    def set_queue_capacity(self, n: int):
        assert n > 0
        self._queue_capacity = n
        for client in self.clients:
            client.set_queue_capacity(self.queue_capacity)
        return self

    @property
    def queue_capacity(self):
        return self._queue_capacity

    @property
    def running(self) -> bool:
        return self._running

    @property
    def ran(self) -> bool:
        return self._ran

    @property
    def round_trip_time(self):
        """Estimated time in minutes it would take to fetch data for all symbols"""
        return int(round((len(self._symbols) / self.clients._rolling_capacity()), 0))
        # return int(round(len(self._symbols) / self.clients.rolling_capacity, 0))

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    def _set_hours(self):
        self.regstart, self.regend = self._get_hours()
        del self._regstart
        del self._regend

    def _get_hours(self) -> Tuple[dt.datetime, dt.datetime]:
        if self._regstart is not None and self._regend is not None:
            assert (
                self._regend > self._regstart
            ), f"regstart >= regend (regstart={self._regstart.isoformat()}, regend={self._regend.isoformat()})"
            assert (self._regend - self._regstart).total_seconds() <= (
                6.75 * 3600
            ), f"regend - regstart > 6.75 hours (timedelta={(self._regend - self._regstart).total_seconds() / 3600}hrs)"
            return self._regstart, self._regend
        elif self._test:
            regstart = dt.datetime.now()
            regend = regstart + dt.timedelta(minutes=self._test_time_minutes)
            self.get_logger().info(f"Testing for {self._test_time_minutes} minutes")
            return regstart, regend
        else:
            hours = mh.fetch_today()
            # hours = mh.fetch_today(client=self.clients._await_client_with_capacity())

            if not mh.isOpen(hours, self.__class__.MARKET):
                self.get_logger().error("Options markets are closed.")
                # self.cleanup()
                sys.exit(-1)

            regstart = mh.get_hour(hours, self.REGSTART_XPATH)
            regend = mh.get_hour(hours, self.REGEND_XPATH)
            return regstart, regend

    @symbols.setter
    def symbols(self, __symbols: List[str]):
        self.get_logger().error(
            "symbols is a read-only property. Use `add_symbol`, `remove_symbol`, "
            "`add_symbols`, `remove_symbols`, or `clear_symbols`"
        )

    def add_symbol(self, symbol: str) -> None:
        if symbol not in self._symbols:
            self.get_logger().info(f"Adding symbol {symbol}")
            self._symbols.append(symbol)

    def add_symbols(self, symbols: List[str]) -> None:
        new_symbols = [s for s in symbols if s not in self._symbols]
        if new_symbols:
            self.get_logger().info(f"Adding symbols {','.join(new_symbols)}")
            self._symbols.extend(new_symbols)

    def remove_symbol(self, symbol: str) -> None:
        if symbol in self._symbols:
            self.get_logger().info(f"Removing symbol {symbol}")
            self._symbols.remove(symbol)
            self._removed_symbols.append(symbol)

    def remove_symbols(self, symbols: List[str]) -> None:
        remove_symbols = [s for s in symbols if s in self._symbols]
        if remove_symbols:
            self.get_logger().info(f"Removing symbols {','.join(remove_symbols)}")
            self._symbols = [s for s in self._symbols if s not in remove_symbols]
            self._removed_symbols.extend(remove_symbols)

    def clear_symbols(self) -> None:
        self.get_logger().info("Clearing symbols")
        self._symbols = []

    async def cleanup(self):
        for client in self.clients:
            client.stop()
            atexit.register(client.logout)

    @property
    def past_regend(self):
        return dt.datetime.now() > self.regend

    @property
    def expected_request_count(self) -> int:
        return int(
            (self.regend - self.regstart).total_seconds()
            / 60
            * self.clients._rolling_capacity()
        )

    @property
    def expected_remaining_request_count(self) -> int:
        return int(
            (self.regend - dt.datetime.now()).total_seconds()
            / 60
            * self.clients._rolling_capacity()
        )

    async def _wait_until_regstart(self):
        self._ran = True

        if (now := dt.datetime.now()) < self.regstart:
            await asyncio.sleep(max((self.regstart - now).total_seconds(), 0.0))

        self._running = True
        self.get_logger().info("Running")
        return self

    def _begin_trip(self) -> float:
        self._round_trip += 1
        self.get_logger().info(f"Starting round trip {self._round_trip}")
        end_time = min(
            (
                dt.datetime.now() + dt.timedelta(minutes=self.round_trip_time)
            ).timestamp(),
            self.regend.timestamp(),
        )
        return end_time

    async def _next_trip(self, end_time):
        sleep_time = end_time - dt.datetime.now().timestamp() - 30.0
        self.get_logger().info(
            f"Completed round trip {self._round_trip}, sleeping for ~{int(sleep_time // 60)} minutes"
        )
        await asyncio.sleep(sleep_time)

    async def stop(self, hard_stop: bool = False):
        self.clients.stop(hard_stop=hard_stop)

        self.get_logger().info("Waiting for clients to finish")
        while not all(c.done for c in self.clients):
            self.get_logger().info(
                json.dumps({c.idx: c.done for c in self.clients}, indent=4)
            )
            await asyncio.sleep(15)

        await self.cleanup()

        self.get_logger().info("Exiting")
        self._running = False
        self.end_event.set()

    async def _listen_for_input(self, stream=sys.stdin):
        self.get_logger().info("Starting")
        while self.running and not self.graceful_exit_event.is_set():
            if message := stream.readline().strip():
                message = json.loads(message)
                match message:
                    case {"cmd": "add_symbol", "symbol": new_symbol}:
                        self.add_symbol(new_symbol)
                    case {"cmd": "drop_symbol", "symbol": existing_symbol}:
                        self.remove_symbol(existing_symbol)
                    case {"cmd": "exit_at_next_sleep"}:
                        pass
                    case {"cmd": "pause"}:
                        pass
                    case {"cmd": "exit"}:
                        self.get_logger().debug('Received "exit" cmd')
                        self.graceful_exit_event.set()

            sys.stdin.flush()
            await asyncio.sleep(1.0)

        self.get_logger().info("Returning")
        return

    def log_performance_metrics(self):
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)
        self.get_logger().info(f"Memory Usage: {mem.percent}% | CPU Usage: {cpu}%")
