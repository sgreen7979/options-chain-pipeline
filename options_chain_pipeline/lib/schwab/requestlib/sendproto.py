#!/usr/bin/env python3
import copy
from typing import (
    Any,
    Callable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)
from typing_extensions import TypedDict

from kafka import KafkaProducer
from pydantic import BaseModel, Field

from daily.types import StrPath

import selectors
import ssl

from kafka.oauth import AbstractTokenProvider
from kafka.partitioner.default import DefaultPartitioner

__all__ = [
    "SendProtocolModel",
    "SocketSendProtocol",
    "HttpSendProtocol",
    "RedisSendProtocol",
    "KafkaSendProtocol",
    "SendProtocolType",
    ##############################
    # "HttpSendProtocolDictType",
    # "KafkaSendProtocolDictType",
    # "RedisSendProtocolDictType",
    # "SocketSendProtocolDictType",
    # "SendProtocolDictType",
    "KakfaProducerConfig",
]


class KakfaProducerConfig(TypedDict):
    bootstrap_servers: Union[str, List[str]]
    client_id: Optional[str]
    key_serializer: Optional[Callable[[Any], bytes]]
    value_serializer: Optional[Callable[[Any], bytes]]
    acks: Literal[0, 1, "all"]
    bootstrap_topics_filter: Set
    compression_type: Optional[str]
    retries: int
    batch_size: int
    linger_ms: int
    partitioner: Union[DefaultPartitioner, Callable]
    buffer_memory: int
    connections_max_idle_ms: int
    max_block_ms: int
    max_request_size: int
    metadata_max_age_ms: int
    retry_backoff_ms: int
    request_timeout_ms: int
    receive_buffer_bytes: Optional[int]
    send_buffer_bytes: Optional[int]
    socket_options: List[Tuple[int, int, int]]
    sock_chunk_bytes: int  # undocumented experimental Optional[optio]n
    sock_chunk_buffer_count: int  # undocumented experimental Optional[optio]n
    reconnect_backoff_ms: int
    reconnect_backoff_max_ms: int
    max_in_flight_requests_per_connection: int
    security_protocol: str
    ssl_context: Optional[ssl.SSLContext]
    ssl_check_hostname: bool
    ssl_cafile: Optional[StrPath]
    ssl_certfile: Optional[StrPath]
    ssl_keyfile: Optional[StrPath]
    ssl_crlfile: Optional[StrPath]
    ssl_password: Optional[str]
    ssl_ciphers: Optional[str]
    api_version: Optional[Tuple[int, int, int]]
    api_version_auto_timeout_ms: int
    metric_reporters: List  # List[type]?
    metrics_num_samples: int
    metrics_sample_window_ms: int
    selector: Type[selectors.BaseSelector]
    sasl_mechanism: Optional[str]
    sasl_plain_username: Optional[str]
    sasl_plain_password: Optional[str]
    sasl_kerberos_service_name: str
    sasl_kerberos_domain_name: Optional[str]
    sasl_oauth_token_provider: Optional[AbstractTokenProvider]


class SendProtocolModel(BaseModel):
    # typeflag: Literal["REDIS", "HTTP", "SOCKET", "KAFKA"]
    type: Literal["REDIS", "HTTP", "SOCKET", "KAFKA"]

    # model_config = {
    #     "populate_by_name": True,  # Allow using internal names
    #     "use_enum_values": True,  # If there are enums, use their values
    #     "alias_generator": None,  # No automatic alias generation
    #     "populate_by_name": True,  # Allows initialization by alias
    # }


class RedisSendProtocol(SendProtocolModel):
    # typeflag: Literal['REDIS'] = "REDIS"
    type: Literal['REDIS'] = "REDIS"
    key: Union[str, bytes]


class HttpSendProtocol(SendProtocolModel):
    # typeflag: Literal['HTTP'] = "HTTP"
    type: Literal['HTTP'] = "HTTP"
    url: str


class KafkaSendProtocol(SendProtocolModel):
    # typeflag: Literal['KAFKA'] = "KAFKA"
    type: Literal['KAFKA'] = "KAFKA"
    topic: str
    kakfa_producer_kwargs: Optional[KakfaProducerConfig] = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def _get_default_kafka_producer_config():
        return copy.deepcopy(KafkaProducer.DEFAULT_CONFIG)

    @staticmethod
    def _get_user_kafka_producer_config():
        from ..producer.base_producer import KAFKA_CONFIG as USER_CONFIG

        kafka_producer_config_keys = list(KafkaProducer.DEFAULT_CONFIG.keys())
        return {k: v for k, v in USER_CONFIG if k in kafka_producer_config_keys}

    def _merge_configs(self):
        configs = self._get_default_kafka_producer_config()
        configs.update(self._get_user_kafka_producer_config())
        if self.kakfa_producer_kwargs is not None:
            configs.update(self.kakfa_producer_kwargs)
        return configs

    def get_kafka_producer(self):
        configs = self._merge_configs()
        return KafkaProducer(**configs)


class SocketSendProtocol(SendProtocolModel):
    # typeflag: Literal['SOCKET'] = "SOCKET"
    type: Literal['SOCKET'] = "SOCKET"
    host: str = Field(default="localhost")
    port: int = Field(default=65432)


SendProtocolType = Union[
    HttpSendProtocol,
    RedisSendProtocol,
    KafkaSendProtocol,
    SocketSendProtocol,
]

# HttpSendProtocolDictType = HttpSendProtocol.__annotations__
# # HttpSendProtocolDictType = Dict[str, HttpSendProtocol.__annotations__]
# # KafkaSendProtocolDictType = Dict[str, KafkaSendProtocol.__annotations__]
# KafkaSendProtocolDictType = KafkaSendProtocol.__annotations__
# # RedisSendProtocolDictType = Dict[str, RedisSendProtocol.__annotations__]
# RedisSendProtocolDictType = RedisSendProtocol.__annotations__
# # SocketSendProtocolDictType = Dict[str, SocketSendProtocol.__annotations__]
# SocketSendProtocolDictType = SocketSendProtocol.__annotations__

# SendProtocolDictType = Union[
#     HttpSendProtocolDictType,
#     RedisSendProtocolDictType,
#     SocketSendProtocolDictType,
#     KafkaSendProtocolDictType,
# ]
