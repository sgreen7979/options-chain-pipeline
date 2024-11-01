from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
)


if TYPE_CHECKING:
    import selectors
    import ssl

    from kafka.metrics.metrics_reporter import AbstractMetricsReporter
    from kafka.oauth.abstract import AbstractTokenProvider


class KafkaConsumerConfigs(TypedDict, total=False):
    bootstrap_servers: Optional[Union[str, List[str]]]
    client_id: Optional[str]
    group_id: Optional[str]
    key_deserializer: Optional[Callable]
    value_deserializer: Optional[Callable]
    fetch_min_bytes: Optional[int]
    fetch_max_wait_ms: Optional[int]
    fetch_max_bytes: Optional[int]
    max_partition_fetch_bytes: Optional[int]
    request_timeout_ms: Optional[int]
    retry_backoff_ms: Optional[int]
    reconnect_backoff_ms: Optional[int]
    reconnect_backoff_max_ms: Optional[int]
    max_in_flight_requests_per_connection: Optional[int]
    auto_offset_reset: Optional[str]
    enable_auto_commit: Optional[bool]
    auto_commit_interval_ms: Optional[int]
    default_offset_commit_callback: Optional[Callable]
    check_crcs: Optional[bool]
    metadata_max_age_ms: Optional[int]
    partition_assignment_strategy: Optional[List[Callable]]
    """
    List of objects to use to distribute partition ownership
    amongst consumer instances when group management is used.
        Default: [RangePartitionAssignor, RoundRobinPartitionAssignor]
    """
    max_poll_records: Optional[int]
    max_poll_interval_ms: Optional[int]
    session_timeout_ms: Optional[int]
    heartbeat_interval_ms: Optional[int]
    receive_buffer_bytes: Optional[int]
    send_buffer_bytes: Optional[int]
    socket_options: Optional[List[Tuple[int]]]
    """
    (list): List of tuple-arguments to socket.setsockopt
        to apply to broker connection sockets. Default:
        [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
    """
    consumer_timeout_ms: Optional[int]
    security_protocol: Optional[
        Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    ]
    ssl_context: Optional["ssl.SSLContext"]
    """
    (Optional[SSLContext]): Pre-configured SSL context for secure connections.
    Allows advanced users to customize SSL settings more finely.
    """
    ssl_check_hostname: Optional[bool]
    ssl_cafile: Optional[str]
    ssl_certfile: Optional[str]
    ssl_keyfile: Optional[str]
    ssl_password: Optional[str]
    ssl_crlfile: Optional[str]
    ssl_ciphers: Optional[str]
    """
    (str): optionally set the available ciphers for ssl
        connections. It should be a string in the OpenSSL cipher list
        format. If no cipher can be selected (because compile-time options
        or other configuration forbids use of all the specified ciphers),
        an ssl.SSLError will be raised. See ssl.SSLContext.set_ciphers
    """
    api_version: Optional[Tuple[int, int, int]]
    """
    (tuple): Specify which Kafka API version to use. If set to
        None, the client will attempt to infer the broker version by probing
        various APIs. Different versions enable different functionality.
    """


class KafkaProducerConfigs(TypedDict, total=False):
    bootstrap_servers: Optional[Union[str, List[str]]]
    client_id: Optional[str]
    key_deserializer: Optional[Callable]
    value_deserializer: Optional[Callable]
    acks: Optional[Literal[0, 1, "all"]]
    compression_type: Optional[str]
    retries: Optional[int]
    batch_size: Optional[int]
    linger_ms: Optional[int]
    partitioner: Optional[Callable]
    buffer_memory: Optional[int]
    connections_max_idle_ms: Optional[int]
    max_block_ms: Optional[int]
    max_request_size: Optional[int]
    metadata_max_age_ms: Optional[int]
    retry_backoff_ms: Optional[int]
    request_timeout_ms: Optional[int]
    receive_buffer_bytes: Optional[int]
    send_buffer_bytes: Optional[int]
    socket_options: Optional[List[Tuple[int]]]
    reconnect_backoff_max_ms: Optional[int]
    reconnect_backoff_ms: Optional[int]
    max_in_flight_requests_per_connection: Optional[int]
    security_protocol: Optional[
        Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    ]
    ssl_cafile: Optional[str]
    ssl_certfile: Optional[str]
    ssl_check_hostname: Optional[bool]
    ssl_ciphers: Optional[str]
    ssl_context: Optional["ssl.SSLContext"]
    """
    (Optional[SSLContext]): Pre-configured SSL context for secure connections.
    Allows advanced users to customize SSL settings more finely.
    """
    ssl_crlfile: Optional[str]
    ssl_keyfile: Optional[str]
    ssl_password: Optional[str]
    api_version: Optional[Tuple[int, int, int]]
    api_version_auto_timeout_ms: Optional[int]
    metric_reporters: Optional[List["AbstractMetricsReporter"]]
    """
    A list of classes to use as metrics reporters.
            Implementing the AbstractMetricsReporter interface allows plugging
            in classes that will be notified of new metric creation. Default: []
    """
    metrics_num_samples: Optional[int]
    metrics_sample_window_ms: Optional[int]
    selector: Optional["selectors.BaseSelector"]
    sasl_mechanism: Optional[
        Literal["PLAIN", "GSSAPI", "OAUTHBEARER", "SCRAM-SHA-256", "SCRAM-SHA-512"]
    ]
    sasl_plain_username: Optional[str]
    sasl_plain_password: Optional[str]
    sasl_kerberos_service_name: Optional[str]
    sasl_kerberos_domain_name: Optional[str]
    sasl_oauth_token_provider: Optional["AbstractTokenProvider"]


class ControlCommand(TypedDict):
    action: Literal["subscribe", "unsubscribe"]
    topics: Set[str]
