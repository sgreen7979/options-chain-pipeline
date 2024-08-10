import datetime as dt
import json
import os
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Union,
    TYPE_CHECKING,
)

from kafka import KafkaAdminClient
from kafka import KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

from daily.schwab.client import ClientGroup
from daily.utils.requests_dataclasses import RequestOutcome
from daily.utils.requests_dataclasses import RequestStatistics

if TYPE_CHECKING:
    import logging
    import requests


class BaseProducer:
    def __init__(self, symbols: Union[List[str], Callable[[], List[str]]]):
        self.clients = ClientGroup()
        self._symbols: List[str] = (
            symbols() if isinstance(symbols, Callable) else symbols
        )

        self.get_logger().info(f"Number of clients {len(self.clients)}")
        self.get_logger().info(
            f"Total rolling request capacity {self.clients.rolling_capacity}"
        )
        self.get_logger().info(f"Number of symbols {len(self._symbols)}")
        self.get_logger().info(
            f"Estimated round trip time {self.round_trip_time} minutes"
        )

        self._removed_symbols: List[str] = []
        self._running = self._ran = False
        self._round_trip = self._batch_num = 0
        self._request_statistics = RequestStatistics()

    @property
    def round_trip_time(self):
        """Estimated time in minutes it would take to fetch data for all symbols"""
        return int(len(self._symbols) // self.clients.rolling_capacity)

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    def get_logger(self) -> "logging.Logger": ...

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

    def _extract_and_add_request_outcome(
        self,
        fetch_time: "dt.datetime",
        symbol: str,
        client_idx: int,
        response: Optional["requests.Response"] = None,
        prepared_request: Optional["requests.PreparedRequest"] = None,
        data_validator: Optional[Callable[[Dict], bool]] = None,
        metadata: Optional[Dict] = None,
    ) -> "RequestOutcome":
        ro = RequestOutcome(
            fetch_time,
            symbol,
            client_idx,
            response,
            prepared_request,
            data_validator,
            metadata,
        )
        self._request_statistics.add_request_outcome(ro)
        return ro

    def cleanup(self):
        for client in self.clients:
            client.logout()


class BaseKafkaProducer(BaseProducer):
    def __init__(
        self,
        symbols: Union[List[str], Callable[[], List[str]]],
        topic_prefix: str,
        bootstrap_servers: Union[str, List[str]],
        compression_type: Optional[str] = None,
        value_serializer: Optional[Callable[[Dict], bytes]] = None,
        num_partitions: int = 1,
        replication_factor: int = 1,
        max_request_size=1_048_576,
    ):
        super().__init__(symbols)
        self._topic_prefix = topic_prefix
        self._bootstrap_servers = bootstrap_servers
        self._compression_type = compression_type
        self._value_serializer = value_serializer
        self._max_request_size = max_request_size
        self.kafka_topic = self._create_kafka_topic(num_partitions, replication_factor)
        self.producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=self._value_serializer
            or (lambda v: json.dumps(v).encode()),
            # client_id=f"OptionsChainProducer-{dt.date.today().strftime('%Y%m%d')}-{str(os.getpid())}",
            compression_type=self._compression_type,
            # max_request_size=1_048_576, # 1_373_859 1_706_746 1_171_695 1_172_717 1_250_731  1_186_854  2_626_686  1_056_610
            # max_request_size=5_000_000,
            max_request_size=self._max_request_size,
        )
        self.get_logger().info(f"kafka topic: {self.kafka_topic}")

    def _create_kafka_topic(
        self,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> str:
        topic_name = f"{self._topic_prefix}_{dt.date.today().strftime('%Y%m%d')}"
        admin_client = KafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        if topic_name in admin_client.list_topics():
            self.get_logger().info(f"Kafka topic '{topic_name}' already exists")
        else:
            topic = NewTopic(topic_name, num_partitions, replication_factor)
            admin_client.create_topics([topic])
            self.get_logger().info(f"Created kafka topic '{topic_name}'")
        admin_client.close()
        return topic_name

    def _to_kafka(self, symbol: str, data: Dict, flush: bool = False):
        def _on_send_success(record_metadata):
            self.get_logger().debug(
                f"Sent {symbol} message to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
            )

        def _on_send_error(excp):
            self.get_logger().error(f'Failed to send message {symbol}', exc_info=excp)

        try:
            self.producer.send(
                self.kafka_topic,
                key=symbol.encode(),
                value=data,
            ).add_callback(_on_send_success).add_errback(_on_send_error)
            # self.get_logger().debug(f"Successfully sent data to Kafka {symbol}")

            if flush:
                self.get_logger().debug("Flushing kafka producer")
                self.producer.flush()

        except KafkaError as e:
            self.get_logger().error(f"Failed to send data to Kafka for {symbol}: {e}")

    # def _on_send_success(self, record_metadata):
    #     self.get_logger().debug(
    #         f"Sent message to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
    #     )

    # def _on_send_error(self, excp):
    #     self.get_logger().error('Failed to send message', exc_info=excp)

    def cleanup(self):
        self.producer.close()
        super().cleanup()
