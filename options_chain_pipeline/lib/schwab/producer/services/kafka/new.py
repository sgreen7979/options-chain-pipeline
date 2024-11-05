import asyncio
from collections import defaultdict
import contextvars
from functools import wraps
from itertools import count, cycle
import multiprocessing
import multiprocessing.context
import multiprocessing.synchronize
from queue import Empty
from random import choice
import threading
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Concatenate,
    DefaultDict,
    Iterable,
    List,
    Optional,
    ParamSpec,
    Self,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    Unpack,
    cast,
    overload,
)

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin.new_topic import NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from kafka.producer.kafka import FutureRecordMetadata

from daily.types import StrPath
from daily.utils.logging import ClassNameLoggerMixin, getLogger

from .models import (
    ControlCommand,
    KafkaConsumerConfigs,
    KafkaProducerConfigs,
    KafkaAdminConfigs,
)
from .utils import (
    get_broker_api_version,
    get_default_consumer_configs,
    get_default_producer_configs,
    KafkaSendCallback,
    _SendResult,
)

if TYPE_CHECKING:
    from kafka.consumer.subscription_state import ConsumerRebalanceListener

_P = ParamSpec("_P")
_RetType = TypeVar("_RetType")
KAFKA_MESSAGE_VAR = contextvars.ContextVar["ConsumerRecord"]("KAFKA_MESSAGE_VAR")
API_VERSION = get_broker_api_version()


class KafkaHandler(ClassNameLoggerMixin):
    """
    This class is designed to handle messages
    retrieved from consumers subscribed to
    particular topic(s)
    """

    LEVEL = "INFO"
    CH = True
    CACHED = True

    CallbackType = Callable[
        Concatenate["ConsumerRecord", _P], Union[Awaitable[_RetType], _RetType]
    ]

    _counters: DefaultDict[Type["KafkaHandler"], Callable[[], int]] = defaultdict(
        lambda: count(start=1).__next__
    )

    @staticmethod
    def get_counter(klass: Type["KafkaHandler"]) -> Callable[[], int]:
        return KafkaHandler._counters[klass]

    @classmethod
    def new_count(cls) -> int:
        counter = KafkaHandler.get_counter(cls)
        return counter()

    def __init__(
        self,
        *topics: str,
        callbacks: Optional[List["KafkaHandler.CallbackType"]] = None,
        name: Optional[str] = None,
    ) -> None:
        """_summary_

        :param callbacks: _description_, defaults to None
        :type callbacks: Optional[List[&quot;KafkaHandler.CallbackType&quot;]], optional
        :param name: _description_, defaults to None
        :type name: Optional[str], optional
        :raises ValueError: _description_
        """
        if not topics:
            raise ValueError("at least one topic must be provided")
        self._topics: Set[str] = set(topics)
        self._callbacks: List[KafkaHandler.CallbackType] = callbacks or []
        self._count: int = self.new_count()
        self._name: str = name or self._get_default_name()

    @property
    def topics(self):
        return self._topics

    @property
    def callbacks(self):
        return self._callbacks

    @property
    def name(self):
        return self._name

    def add_topic(self, topic: str):
        self._topics.add(topic)

    def add_callback(
        self,
        callback: "KafkaHandler.CallbackType",
    ) -> Self:
        if callback not in self._callbacks:
            self._callbacks.append(callback)
        return self

    def _get_default_name(self):
        return "{}-{}".format(type(self).__name__, self._count)

    def _wrap_callback(
        self,
        callback: Callable[
            Concatenate["ConsumerRecord", _P],
            Union[Awaitable[_RetType], _RetType],
        ],
    ) -> Callable[_P, Union[Awaitable[_RetType], _RetType]]:
        """Wrap the callback to inject a ConsumerRecord as the first parameter."""

        @wraps(callback)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs):
            message = KAFKA_MESSAGE_VAR.get()
            return callback(message, *args, **kwargs)

        return wrapper

    def __contains__(self, topic: str):
        return topic in self.topics

    def _is_relevant_message(self) -> bool:
        message = KAFKA_MESSAGE_VAR.get()
        return message.topic in self.topics

    async def fire_callbacks(self, *args, **kwargs):
        if self._is_relevant_message():
            for callback in self.callbacks:
                wrapped_callback = self._wrap_callback(callback)
                ret = wrapped_callback(*args, **kwargs)
                if asyncio.iscoroutine(ret):
                    ret = await ret


class ScopedService:
    """
    This class represents a scoped Kafka
    service containing one instance each
    of KafkaProducer and KafkaConsumer and
    handlers to manage the messages retrived
    from the consumer and it assigned topics.
    """

    _instances: ClassVar[Set["ScopedService"]] = set["ScopedService"]()

    @classmethod
    def get_active_instances(cls):
        active = []
        for inst in cls._instances:
            if inst.started() and not inst.stopped():
                active.append(inst)
        return active

    def __init__(
        self,
        service_handle: "KafkaService",
        producer: KafkaProducer,
        consumer: KafkaConsumer,
        handlers: Optional[Iterable[KafkaHandler]] = None,
    ):
        """Initialize a scoped kafka service.

        :param service: the KafkaService object_
        :type service: KafkaService
        :param producer: the producer
        :type producer: KafkaProducer
        :param consumer: the consumer
        :type consumer: KafkaConsumer
        :param handlers: a list of handlers, defaults to None
        :type handlers: Optional[Iterable[KafkaHandler]], optional
        """
        self._service = service_handle
        self._producer = producer
        self._consumer = consumer
        self._handlers = set(handlers or [])
        self.__is_caller = self._started = False
        self._logger = getLogger(
            self._service.get_logger().name + self.__class__.__name__
        )
        self._handlers_lock: threading.RLock = threading.RLock()
        self._listener_task: asyncio.Task
        self._detached: bool = False
        self.__class__._instances.add(self)

    def publish(
        self,
        topic: str,
        value: Optional[Union[bytes, Any]] = None,
        key: Optional[bytes] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        *,
        flush: bool = False,
        errbacks: Optional[List[Callable[["FutureRecordMetadata"], Any]]] = None,
        callbacks: Optional[List[Callable[["FutureRecordMetadata"], Any]]] = None,
    ) -> _SendResult:
        return self._service.publish(
            topic=topic,
            producer=self._producer,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms,
            flush=flush,
            errbacks=errbacks,
            callbacks=callbacks,
        )

    def subscribe(
        self,
        *topics: str,
        pattern: Optional[str] = None,
        listener: Optional["ConsumerRebalanceListener"] = None,
    ):
        self._service.subscribe(
            self._consumer, *topics, pattern=pattern, listener=listener
        )
        with self._handlers_lock:
            for handler in iter(self._handlers):
                handler._topics.update(*topics)

    def unsubscribe(self, *topics: str):
        self._service.unsubscribe(self._consumer, *topics)
        with self._handlers_lock:
            for handler in iter(self._handlers):
                handler._topics.difference_update(*topics)

    def subscriptions(self):
        return self._service._subscription_handler._get_current_subscriptions(
            self._consumer
        )

    def subscribed_to(self, topic: str):
        return self._service.subscribed_to(topic, consumer=self._consumer)

    def add_handler(self, handler: KafkaHandler):
        if handler not in self._handlers:
            self._handlers.add(handler)
            for topic in handler.topics:
                if not self.subscribed_to(topic):
                    self.subscribe(topic)
        self._service.add_handler(handler, auto_subscribe=False)

    @classmethod
    def from_configs(
        cls,
        service: "KafkaService",
        producer_configs: KafkaProducerConfigs,
        consumer_configs: KafkaConsumerConfigs,
    ):
        producer = service.initialize_producer(**producer_configs)
        consumer = service.initialize_consumer(**consumer_configs)
        return cls(service, producer, consumer)

    def started(self):
        # return bool(list(self._service.consumer_assignment_map[self._consumer]))
        return self._started

    def stopped(self):
        return self.started() and self._consumer._closed

    def start(self, *, timeout_ms: int = 1000, to_thread: bool = False):
        if not self.started():
            self.__is_caller = True
            if to_thread:
                loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._service.listen,
                    args=(self._consumer),
                    kwargs={"timeout_ms": timeout_ms, "loop": loop},
                )
                self._thread.start()
            else:
                self._service.listen(self._consumer, timeout_ms=timeout_ms)
            self._started = True
            self.__is_caller = False

    def is_owner(self, item: Union[KafkaConsumer, KafkaProducer, KafkaHandler]):
        if isinstance(item, KafkaConsumer):
            return item is self._consumer
        elif isinstance(item, KafkaProducer):
            return item is self._producer
        elif isinstance(item, KafkaHandler):
            return item in self._handlers
        else:
            return False

    def detach(self):
        """
        Detach this scoped service from its underlying
        service handle.

        Once it is released, the consumer(s), producer(s),
        and handler(s) are removed from from the corresponding
        caches maintained by the underlying service handle,
        and the responsibility of cleaning up these resources
        is then passed to owner of this scoped service (the
        user).
        """
        assert (
            self.started() and not self.stopped()
        ), "Cannot detach an inactive scoped service"
        assert not self._detached, "Cannot detach an already detached scoped service"
        return self._service._detach_scoped_service(self)


class KafkaConfigManager(ClassNameLoggerMixin):
    LEVEL = "INFO"
    CH = True
    CACHED = True

    DEFAULT_BOOTSTRAP_SERVERS: Union[str, List[str]] = "localhost:9092"
    DEFAULT_BROKER_API_VERSION: Tuple[int, int, int] = API_VERSION

    DEFAULT_PRODUCER_CONFIGS = get_default_producer_configs(
        DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_BROKER_API_VERSION
    )
    DEFAULT_CONSUMER_CONFIGS = get_default_consumer_configs(
        DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_BROKER_API_VERSION
    )

    @classmethod
    def set_default_bootstrap_servers(cls, bootstrap_servers: Union[str, List[str]]):
        """
        Set `cls.DEFAULT_BOOTSTRAP_SERVERS` and update the
        `"bootstrap_servers"` fields of
        `cls.DEFAULT_PRODUCER_CONFIGS` and
        `cls.DEFAULT_CONSUMER_CONFIGS`

        If the passed bootstrap_servers is not different from the
        currently set value, this function does nothing.
        """
        if bootstrap_servers != cls.DEFAULT_BOOTSTRAP_SERVERS:
            cls.DEFAULT_BOOTSTRAP_SERVERS = bootstrap_servers
            cls.DEFAULT_CONSUMER_CONFIGS["bootstrap_servers"] = (
                cls.DEFAULT_BOOTSTRAP_SERVERS
            )
            cls.DEFAULT_PRODUCER_CONFIGS["bootstrap_servers"] = (
                cls.DEFAULT_BOOTSTRAP_SERVERS
            )

    @classmethod
    def set_default_broker_api_version(cls, api_version: Tuple[int, int, int]):
        """
        Set `cls.DEFAULT_BROKER_API_VERSION` and update the
        `"api_version"` fields of
        `cls.DEFAULT_PRODUCER_CONFIGS` and
        `cls.DEFAULT_CONSUMER_CONFIGS`

        If the passed api_version is not different from the
        currently set value, this function does nothing.
        """
        if api_version != cls.DEFAULT_BROKER_API_VERSION:
            cls.DEFAULT_BROKER_API_VERSION = api_version
            cls.DEFAULT_CONSUMER_CONFIGS["api_version"] = cls.DEFAULT_BROKER_API_VERSION
            cls.DEFAULT_PRODUCER_CONFIGS["api_version"] = cls.DEFAULT_BROKER_API_VERSION

    @overload
    @staticmethod
    def _get_configs(
        service: "KafkaService",
        client_type: Type[KafkaConsumer],
        set_as_default: bool = False,
        **configs: Unpack[KafkaConsumerConfigs],
    ) -> KafkaConsumerConfigs: ...

    @overload
    @staticmethod
    def _get_configs(
        service: "KafkaService",
        client_type: Type[KafkaProducer],
        set_as_default: bool = False,
        **configs: Unpack[KafkaProducerConfigs],
    ) -> KafkaProducerConfigs: ...

    @overload
    @staticmethod
    def _get_configs(
        service: "KafkaService",
        client_type: Type[KafkaAdminClient],
        set_as_default: bool = False,
        **configs: Unpack[KafkaAdminConfigs],
    ) -> KafkaAdminConfigs: ...

    @staticmethod
    def _get_configs(
        service: "KafkaService",
        client_type: Union[
            Type[KafkaConsumer], Type[KafkaProducer], Type[KafkaAdminClient]
        ],
        set_as_default: bool = False,
        **configs,
    ) -> Union[KafkaConsumerConfigs, KafkaProducerConfigs, KafkaAdminConfigs]:
        """Gather and return the initialization configs
        for a given Kafka client_type.

        This function will first set the configs to a
        copy of those currently set in the given service
        (`_consumer_configs` if client_type is
        `KafkaConsumer`, `_producer_configs` if
        client_type is `KafkaProducer`, and a dict
        with the default bootstrap_servers and api_version
        if client_type is `KafkaAdminClient`) and then
        update them for the configs passed.

        If `set_as_default` is True and client_type is
        either `KafkaConsumer` or `KafkaProducer`, we
        update the corresponding configs currently set
        in the given service to reflect those
        generated by this invocation.  If client_type
        is `KafkaAdminClient`, this parameter is ignored.

        :param client_type: the Kafka client type
        :type client_type: Union[Type[KafkaConsumer], Type[KafkaProducer], Type[KafkaAdminClient]]
        :param set_as_default: save the generated configs
            for future use, defaults to False. This
            only applies if client_type is either
            KafkaProducer or KafkaConsumer.
        :type set_as_default: bool, optional
        :raises TypeError: if client_type is not KafkaConsumer,
            KafkaProducer, or KafkaAdminClient
        :return: the configs
        :rtype: Union[KafkaConsumerConfigs, KafkaProducerConfigs, KafkaAdminConfigs]
        """
        if client_type is KafkaConsumer:
            _configs = service._consumer_configs.copy()
            _configs.update(cast(KafkaConsumerConfigs, configs))
            if set_as_default:
                service._consumer_configs = _configs.copy()
        elif client_type is KafkaProducer:
            _configs = service._producer_configs.copy()
            _configs.update(cast(KafkaProducerConfigs, configs))
            if set_as_default:
                service._producer_configs = _configs.copy()
        elif client_type is KafkaAdminClient:
            _configs = cast(KafkaAdminConfigs, {})
            _configs.setdefault("bootstrap_servers", service._bootstrap_servers)
            _configs.setdefault("api_version", service._api_version)
            _configs.update(cast(KafkaAdminConfigs, configs))
        else:
            raise TypeError(
                "Expected type KafkaConsumer, type KafkaProducer, or "
                f"type KafkaAdminClient, got {client_type!r}"
            )
        return _configs

    @staticmethod
    def _get_default_configs(
        service: "KafkaService",
        *,
        bootstrap_servers: Optional[Union[str, List[str]]] = None,
        api_version: Optional[Tuple[int, int, int]] = None,
        config_file: Optional[StrPath] = None,
    ):
        service._api_version = (
            api_version or KafkaConfigManager.DEFAULT_BROKER_API_VERSION
        )
        service._bootstrap_servers = (
            bootstrap_servers or KafkaConfigManager.DEFAULT_BOOTSTRAP_SERVERS
        )
        service._consumer_configs = get_default_consumer_configs(
            service._bootstrap_servers, service._api_version
        ).copy()
        service._producer_configs = get_default_producer_configs(
            service._bootstrap_servers, service._api_version
        ).copy()

        if config_file:
            loaded_configs = KafkaConfigManager.load_config_file(config_file)
            KafkaConfigManager.validate_configdict(loaded_configs)
            # TODO merge the validated loaded_configs with those of the given KafkaService handle

    @staticmethod
    def load_config_file(filepath: StrPath):
        """
        The implementation of this function was thrown
        together without much thought, other than it
        was meant to remain consistent with the form
        in which kafka expects config files (i.e.,
            '''

            some.config.key = some_value

            some.other.config.key = some_other_value

            '''
        )

        We need to implement validation of the parsed
        configs dictionary returned by this function,
        and then merge the config items with those of
        the given KafkaService handle.
        """
        with open(filepath, "r") as config_file:
            contents = config_file.read()

        configs = {}
        for line in contents.splitlines():
            if line.strip() and not line.strip().startswith("#"):
                try:
                    config_key, config_value = line.split("=", 1)
                except ValueError:
                    KafkaConfigManager.get_logger().error(
                        "Failed to parse line in config file: "
                        f"{line}. Skipping line."
                    )
                else:
                    configs[config_key.strip().replace(".", "_")] = config_value.strip()
        return configs

    @staticmethod
    def validate_configdict(configdict: dict):
        """
        TODO need to implement this
        """
        pass


class IncrementalSubscriptionHandler:
    """
    This class provides convenience functions
    for incremental KafkaConsumer subscriptions.

    As currently constructed, calls to
    `~kafka.KafkaConsumer.subscribe` are not
    incremental. Each call effectively replaces
    the current subscriptions with new
    subscriptions to the given topics.

    Calls to this class's `~daily.schwab.producer.services.kafka.IncrementalSubscriptionHandler`
    `subscribe` method preserve previously
    existing subscriptions by adding subcriptions
    to the given the topics. Calls to the
    `unsubscribe` method are functionally
    equivalent but reversed--we update the
    previously existing subscriptions to
    exclude those to the given topics.

    Each of these methods then passes the
    updated set of topics to the internal
    `~daily.schwab.producer.services.kafka.IncrementalSubscriptionHandler._subscribe`
    method, which mirrors calls to the currently
    implemented `~kafka.KafkaConsumer.subscribe`
    method--subscriptions to the given topics
    replace those previously existing.
    """

    def __init__(self, service_handle: "KafkaService") -> None:
        self._service = service_handle
        self._logger = getLogger(
            self._service.get_logger().name + self.__class__.__name__
        )

    def subscribe(
        self,
        consumer: KafkaConsumer,
        *topics: str,
        pattern: Optional[str] = None,
        listener: Optional["ConsumerRebalanceListener"] = None,
    ) -> None:
        """
        :param topics: Topics for subscription.
        :type topics: str
        :param pattern: Pattern to match available topics, defaults to None.
            You must provide either topics or pattern, but not both.
        :type pattern: str, optional
        :param listener: a listener or listener callback, defaults to None.
            The listener will be called before and after each
            rebalance operation.
        :type listener: ConsumerRebalanceListener, optional

            As part of group management, the consumer will keep track
            of the list of consumers that belong to a particular group
            and will trigger a rebalance operation if one of the
            following events trigger:

            * Number of partitions change for any of the subscribed topics
            * Topic is created or deleted
            * An existing member of the consumer group dies
            * A new member is added to the consumer group

            When any of these events are triggered, the provided
            listener will be invoked first to indicate that the
            consumer's assignment has been revoked, and then again
            when the new assignment has been received. Note that
            this listener will immediately override any listener set
            in a previous call to subscribe. It is guaranteed,
            however, that the partitions revoked/assigned through
            this interface are from topics subscribed in this call.

        :raises IllegalStateError: If called after previously calling
            ~kafka.KafkaConsumer.assign.
        :raises AssertionError: If neither topics nor pattern is
            provided or consumer was never set
        :raises TypeError: If listener is not a
            ConsumerRebalanceListener.

        """
        # `consumer.subscribe` is not incremental, meaning every time we
        # call it, the topics we pass to it will be the *only* topics
        # the consumer is actually subscribed to; so we pass it our aggregated set
        # self.consumer.subscribe(topics=self._topics, pattern=pattern, listener=listener)
        # if self._service._is_multiprocess:
        if (
            listener_process := self._service._consumer_listener_process_map[consumer]
        ) is not None:
            if listener_process.is_alive():
                self._service.control_queue.put(
                    {"action": "subscribe", "topics": set(topics)}
                )
        else:
            subs = self._get_current_subscriptions(consumer)
            subs.update(topics)
            self._subscribe(consumer, *subs, pattern=pattern, listener=listener)

    def _subscribe(
        self, consumer: KafkaConsumer, *topics: str, pattern=None, listener=None
    ):
        prev_subcs = self._get_current_subscriptions(consumer)
        consumer.subscribe(topics, pattern=pattern, listener=listener)
        self._service.get_logger().info(
            f"Consumer initially subscribed to {prev_subcs} and is "
            f"now subscribed to {self._get_current_subscriptions(consumer)}"
        )

    def unsubscribe(self, consumer: KafkaConsumer, *topics: str):
        if (
            listener_process := self._service._consumer_listener_process_map[consumer]
        ) is not None:
            if listener_process.is_alive():
                self._service.control_queue.put(
                    {"action": "unsubscribe", "topics": set(topics)}
                )
        else:
            subs = self._get_current_subscriptions(consumer)
            subs -= set(topics)
            self._subscribe(consumer, *subs)

    @staticmethod
    def _get_current_subscriptions(consumer: KafkaConsumer) -> Set[str]:
        """Returns a set of topics to which a given consumer
        is currently subscribed

        :return: the currently subscribed topics
        :rtype: Set[str]
        """
        return consumer.subscription() or set()


class KafkaService(ClassNameLoggerMixin):
    """
    This class is designed to provide a user-friendly
    interface that facilitates instances of ~.ScopedService,
    which itself implements the consumer, producer,
    handler model, and central configuration handling
    for all KafkaClient variants via KafkaConfigManager.

    Features of this class include:

        *Multiprocessing via `start_listener_process`*
        *Async iteration via `listen` when not multiprocessing*
    """

    LEVEL = "INFO"
    CH = True
    CACHED = True

    ConfigManager = KafkaConfigManager()

    def __init__(
        self,
        *,
        bootstrap_servers: Optional[Union[str, List[str]]] = None,
        api_version: Optional[Tuple[int, int, int]] = None,
        config_file: Optional[StrPath] = None,
    ) -> None:
        self._bootstrap_servers: Union[str, List[str]]
        self._api_version: Tuple[int, int, int]
        self._consumer_configs: KafkaConsumerConfigs = {}
        self._producer_configs: KafkaProducerConfigs = {}
        self._config_file: Optional[StrPath] = config_file
        self.ConfigManager._get_default_configs(
            self,
            bootstrap_servers=bootstrap_servers,
            api_version=api_version,
            config_file=config_file,
        )
        self._subscription_handler = IncrementalSubscriptionHandler(self)

        self._handlers: List[KafkaHandler] = []

        self._consumers: Set[KafkaConsumer] = set[KafkaConsumer]()
        self._producers: Set[KafkaProducer] = set[KafkaProducer]()

        self._consumer_listener_task_map: DefaultDict[
            KafkaConsumer, Optional[asyncio.Task]
        ] = defaultdict[KafkaConsumer, Optional[asyncio.Task]](lambda: None)

        self._consumer_listener_process_map: DefaultDict[
            KafkaConsumer, Optional[multiprocessing.Process]
        ] = defaultdict[KafkaConsumer, Optional[multiprocessing.Process]](lambda: None)

        self._poll_timeout_ms: int = 1000

        self._is_multiprocess: bool = False

        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._queue: asyncio.Queue[ConsumerRecord] = asyncio.Queue[ConsumerRecord]()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._queue_listener_started: bool = False

        # We use a _lock to ensure thread-safety when updating the
        # _handlers and _topics attributes
        self._handlers_and_topics_lock: threading.RLock = threading.RLock()
        self._consumers_lock: threading.RLock = threading.RLock()
        self._producers_lock: threading.RLock = threading.RLock()

    def has_consumer(self) -> bool:
        return bool(self._consumers)

    def has_producer(self) -> bool:
        return bool(self._producers)

    @property
    def consumer_assignment_map(self) -> DefaultDict[KafkaConsumer, Set[str]]:
        """A mapping of consumers to topics to which they are
        currently subscribed.

        :return: the consumer assignment map
        :rtype: DefaultDict[KafkaConsumer, Set[str]]
        """
        consumer_assignment_map: DefaultDict[KafkaConsumer, Set[str]] = defaultdict[
            KafkaConsumer, Set[str]
        ](set[str])

        for consumer in self._consumers:
            if not consumer._closed:
                consumer_assignment_map[consumer] = (
                    self._subscription_handler._get_current_subscriptions(consumer)
                )
        return consumer_assignment_map

    def subscribed_to(
        self, topic: str, consumer: Optional[KafkaConsumer] = None
    ) -> bool:
        """Whether any consumers are currently subscribed to
        a given topic

        :param topic: the topic
        :type topic: str
        :param consumer: the KafkaConsumer, defaults to None
        :type consumer: KafkaConsumer, optional
        :return: whether the given consumer is subscribed to the given topic
            or, if consumer is not provided, whether any consumers are
            currently subscribed to the topic
        :rtype: bool
        """
        if consumer is not None:
            return topic in self._subscription_handler._get_current_subscriptions(
                consumer
            )

        for consumer in self._consumers:
            if topic in self._subscription_handler._get_current_subscriptions(consumer):
                return True
        return False

    @property
    def loop(self):
        return self._loop

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    @property
    def bootstrap_servers(self):
        return self._bootstrap_servers

    @property
    def api_version(self):
        return self._api_version

    def add_consumer(self, consumer: KafkaConsumer):
        if consumer in self._consumers:
            self.get_logger().warning(f"Consumer already saved {consumer!r}")
        else:
            with self._consumers_lock:
                self._consumers.add(consumer)

    def add_producer(self, producer: KafkaProducer):
        if producer in self._producers:
            self.get_logger().warning(f"Producer already saved {producer!r}")
        else:
            with self._producers_lock:
                self._producers.add(producer)

    def add_handler(self, handler: KafkaHandler, *, auto_subscribe: bool = True):
        if handler not in self._handlers:
            with self._handlers_and_topics_lock:
                self._handlers.append(handler)

            if auto_subscribe:
                if self._is_multiprocess is True and not self.mpevent.is_set():
                    self.control_queue.put(
                        ControlCommand(action="subscribe", topics=handler.topics)
                    )
                else:
                    for topic in handler.topics:
                        if not self.subscribed_to(topic):
                            assert self._consumers, "No consumers have been initialized"
                            self.get_logger().warning(
                                "No consumers are currently subscribed "
                                f"to topic {topic}. Assigning the topic "
                                " a random consumer..."
                            )
                            consumer = choice(list(self._consumers))
                            self.subscribe(consumer, topic)

    @overload
    def _initialize_client(
        self,
        client_type: Type[KafkaConsumer],
        set_as_default: bool = False,
        *topics: str,
        **configs: Unpack[KafkaConsumerConfigs],
    ) -> KafkaConsumer: ...

    @overload
    def _initialize_client(
        self,
        client_type: Type[KafkaProducer],
        set_as_default: bool = False,
        **configs: Unpack[KafkaProducerConfigs],
    ) -> KafkaProducer: ...

    @overload
    def _initialize_client(
        self,
        client_type: Type[KafkaAdminClient],
        set_as_default: bool = False,
        **configs: Unpack[KafkaAdminConfigs],
    ) -> KafkaAdminClient: ...

    def _initialize_client(
        self,
        client_type: Union[
            Type[KafkaConsumer], Type[KafkaProducer], Type[KafkaAdminClient]
        ],
        set_as_default: bool = False,
        *topics: str,
        **configs,
    ) -> Union[KafkaConsumer, KafkaProducer, KafkaAdminClient]:
        """Initialize a new instance of a given Kafka
        client type.

        :param client_type: the Kafka client type
        :type client_type: Union[Type[KafkaConsumer], Type[KafkaProducer]]
        :param set_as_default: save the generated configs
            for future use, defaults to False
        :type set_as_default: bool, optional
        :param topics: the topics to pass to the client type constructor,
            defaults to None. Only applicable if client_type is
            KafkaConsumer.
        :type topics: Tuple[str], optional
        :raises TypeError: if client_type is not KafkaConsumer or
            KafkaProducer
        :return: the Kafka client
        :rtype: Union[KafkaConsumer, KafkaProducer]
        """
        _configs = self.ConfigManager._get_configs(
            self, client_type, set_as_default, **configs
        )
        if client_type is KafkaConsumer and topics:
            return client_type(*topics, **_configs)
        else:
            return client_type(**_configs)

    def initialize_producer(
        self,
        set_as_default: bool = False,
        **producer_configs: Unpack[KafkaProducerConfigs],
    ) -> KafkaProducer:
        """Initialize a producer, add it to the `_producers`
        set, and return it.

        If `set_as_default` is True, we update the
        "_producer_configs" attribute to reflect those
        generated by this invocation.

        :param set_as_default: save the generated configs
            for future use, defaults to False
        :type set_as_default: bool, optional
        :param producer_configs: the producer configs
        :type producer_configs: KafkaProducerConfigs
        :return: the new KafkaProducer instance
        :rtype: KafkaProducer
        """
        producer = self._initialize_client(
            KafkaProducer, set_as_default, **producer_configs
        )
        with self._producers_lock:
            self._producers.add(producer)
        return producer

    def initialize_consumer(
        self,
        *topics: str,
        set_as_default: bool = False,
        **consumer_configs: Unpack[KafkaConsumerConfigs],
    ) -> KafkaConsumer:
        """Initialize a consumer, add it to the `_consumers`
        set, and return it.

        If `set_as_default` is True, we update the
        "_consumer_configs" attribute to reflect those
        generated by this invocation.

        :param set_as_default: save the generated configs
            for future use, defaults to False
        :type set_as_default: bool, optional
        :param topics: the topics to pass to the constructor
            of the new consumer instance
        :type topics: str, optional
        :param consumer_configs: the producer configs
        :type consumer_configs: KafkaConsumerConfigs
        :return: the new KafkaConsumer instance
        :rtype: KafkaConsumer
        """
        consumer = self._initialize_client(
            KafkaConsumer, set_as_default, *topics, **consumer_configs
        )

        # with self._handlers_and_topics_lock:
        #     self._topics.update(topics)

        with self._consumers_lock:
            self._consumers.add(consumer)

        return consumer

    def initialize_admin_client(self, **configs: Unpack[KafkaAdminConfigs]):

        if hasattr(self, "_admin_client") and not self._admin_client._closed:
            return self._admin_client

        self._admin_client = self._initialize_client(KafkaAdminClient, **configs)
        return self._admin_client

    def initialize_scoped_service(
        self,
        *topics,
        producer_configs: KafkaProducerConfigs = {},
        consumer_configs: KafkaConsumerConfigs = {},
        handler_callbacks: Optional[List[Callable[["ConsumerRecord"], Any]]] = None,
    ) -> ScopedService:
        producer = self.initialize_producer(**producer_configs)
        consumer = self.initialize_consumer(*topics, **consumer_configs)
        if handler_callbacks:
            handler = KafkaHandler(*topics, callbacks=handler_callbacks)
            self.add_handler(handler)
        return ScopedService(self, producer, consumer, handlers=[handler])

    def _detach_scoped_service(self, scoped_service: ScopedService) -> ScopedService:

        if scoped_service._producer in self._producers:
            with self._producers_lock:
                self._producers.remove(scoped_service._producer)

        if scoped_service._consumer in self._consumers:
            with self._consumers_lock:
                self._consumers.remove(scoped_service._consumer)

        for handler in scoped_service._handlers:
            if handler in self._handlers:
                with self._handlers_and_topics_lock:
                    self._handlers.remove(handler)

        listener_task = self._consumer_listener_task_map.pop(scoped_service._consumer)
        if listener_task is not None:
            scoped_service._listener_task = listener_task

        return scoped_service

    def new_topic(
        self,
        topic: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        *,
        close_admin_client: bool = True,
    ):
        admin_client = self.initialize_admin_client()

        if topic not in admin_client.list_topics():
            admin_client.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                ]
            )

        if close_admin_client:
            admin_client.close()

    async def fire_handlers(self):
        tasks = [handler.fire_callbacks() for handler in self._handlers]
        await asyncio.gather(*tasks)

    def _get_consumers_cycle(self):
        assert self._consumers, "no consumers were saved"
        return cycle(self._consumers)

    async def queue_listener(self):

        self.get_logger().info("queue_listener started")

        while not self._stop_event.is_set():
            try:
                threaded_get_coro = await asyncio.to_thread(self._queue.get)
                message = await threaded_get_coro
            except Exception as e:
                self.get_logger().error(
                    f"Exception raised in queue_listener: {e}", exc_info=True
                )
            else:
                self.get_logger().info(
                    f"Retrieved message from the queue (topic={message.topic}, "
                    f"offset={message.offset})"
                )
                KAFKA_MESSAGE_VAR.set(message)
                await self.fire_handlers()
            finally:
                await asyncio.sleep(1.0)

        self.get_logger().info("queue_listener exiting")

    def _start_close_all_waiter(self):
        if not hasattr(self, "_close_all_task"):
            loop = self._ensure_loop()
            self._close_all_task = loop.create_task(self._close_all_waiter())

    def start_queue_listener(self):
        if not self._queue_listener_started:
            loop = self._ensure_loop()
            task = loop.create_task(self.queue_listener())
            self._queue_listener_task = task
            self._start_close_all_waiter()
            return task

    async def _listen(
        self,
        consumer: KafkaConsumer,
        *,
        timeout_ms: int = 1000,
        stop_event: Optional[asyncio.Event] = None,
    ):
        stop_event = stop_event or self._stop_event
        while not (consumer._closed or stop_event.is_set()):
            message_batch = consumer.poll(timeout_ms=self._poll_timeout_ms)
            for tp, messages in message_batch.items():
                for message in iter(messages):
                    message = cast(ConsumerRecord, message)
                    self.get_logger().info(
                        f"Received message from {consumer!r}: "
                        f"(topic={message.topic}, offset={message.offset})"
                    )
                    await self._queue.put(message)

            await asyncio.sleep(timeout_ms / 1000)

        self.get_logger().info(f"Exiting listen loop for conusmer {consumer!r}")

    def listen(
        self,
        consumer: KafkaConsumer,
        *,
        timeout_ms: int = 1000,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        if self._consumer_listener_task_map[consumer] is None:
            for pair in iter(ScopedService._instances):
                if (
                    pair.is_owner(consumer)
                    and not pair.__is_caller
                    and not pair.started()
                ):
                    raise RuntimeError(
                        "Attempting to start a listener on a consumer that "
                        "is owned by a KafkaPair instance. Please call the "
                        "KafkaPair instance's `start` method to begin "
                        "listening to the consumer."
                    )
                else:
                    break

            self.start_queue_listener()
            if loop is None:
                loop = self._ensure_loop()
            else:
                asyncio.set_event_loop(loop)

            listener_task = loop.create_task(
                self._listen(consumer, timeout_ms=timeout_ms, stop_event=stop_event)
            )
            if stop_event:
                setattr(listener_task, "_stop_event", stop_event)

            self._consumer_listener_task_map[consumer] = listener_task

    async def stop_listening_to(self, consumer: KafkaConsumer):
        listener_task = self._consumer_listener_task_map[consumer]
        if listener_task is not None:
            try:
                if (event := getattr(listener_task, "_stop_event", None)) is not None:
                    if not event.is_set():
                        event.set()
                else:
                    listener_task.cancel()

                await listener_task
            except Exception as e:
                self.get_logger().error(
                    f"Error while trying to stop listener task: {e}", exc_info=True
                )
            finally:
                if listener_task.done():
                    del self._consumer_listener_task_map[consumer]

    def _ensure_loop(self):
        if self._loop is None or self._loop.is_closed():
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._loop_owner = True
        return self._loop

    async def mpqueue_listener(self):
        assert self.mpqueue is not None, "mpqueue was never set"
        assert self.mpevent is not None, "mpevent was never set"
        try:
            while not self.mpevent.is_set():
                # message = await asyncio.to_thread(self.mpqueue.get)  # Fetch from queue
                try:
                    message = self.mpqueue.get_nowait()
                except Empty:
                    pass
                else:
                    KAFKA_MESSAGE_VAR.set(message)
                    await self.fire_handlers()
                finally:
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            self.get_logger().info("mpqueue_listener task was cancelled.")
            raise  # propogate the error

        except (EOFError, BrokenPipeError):
            self.get_logger().info("mpqueue has been closed. Stopping listener.")

    def subscribe(
        self,
        consumer: KafkaConsumer,
        *topics: str,
        pattern: Optional[str] = None,
        listener: Optional["ConsumerRebalanceListener"] = None,
    ) -> None:
        """
        :param topics: Topics for subscription.
        :type topics: str
        :param pattern: Pattern to match available topics, defaults to None.
            You must provide either topics or pattern, but not both.
        :type pattern: str, optional
        :param listener: a listener or listener callback, defaults to None.
            The listener will be called before and after each
            rebalance operation.
        :type listener: ConsumerRebalanceListener, optional

            As part of group management, the consumer will keep track
            of the list of consumers that belong to a particular group
            and will trigger a rebalance operation if one of the
            following events trigger:

            * Number of partitions change for any of the subscribed topics
            * Topic is created or deleted
            * An existing member of the consumer group dies
            * A new member is added to the consumer group

            When any of these events are triggered, the provided
            listener will be invoked first to indicate that the
            consumer's assignment has been revoked, and then again
            when the new assignment has been received. Note that
            this listener will immediately override any listener set
            in a previous call to subscribe. It is guaranteed,
            however, that the partitions revoked/assigned through
            this interface are from topics subscribed in this call.

        :raises IllegalStateError: If called after previously calling
            ~kafka.KafkaConsumer.assign.
        :raises AssertionError: If neither topics nor pattern is
            provided or consumer was never set
        :raises TypeError: If listener is not a
            ConsumerRebalanceListener.

        """
        self._subscription_handler.subscribe(
            consumer,
            *topics,
            pattern=pattern,
            listener=listener,
        )

    def unsubscribe(self, consumer: KafkaConsumer, *topics: str):
        self._subscription_handler.unsubscribe(consumer, *topics)

    def publish(
        self,
        topic: str,
        value: Optional[Union[bytes, Any]] = None,
        key: Optional[bytes] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        *,
        producer: Optional[KafkaProducer] = None,
        flush: bool = False,
        errbacks: Optional[List[Callable[["FutureRecordMetadata"], Any]]] = None,
        callbacks: Optional[List[Callable[["FutureRecordMetadata"], Any]]] = None,
    ) -> _SendResult:
        """
        :param topic: topic where the message will be published
        :type topic: str
        :param value: message value, optional
        :type value: bytes or a value that is serializable to
            bytes via the configured `value_serializer`. If value
            is None, key is required and message acts as a 'delete'.
            See kafka compaction documentation for more details:
                https://kafka.apache.org/documentation.html#compaction (compaction requires kafka >= 0.8.1)
        :param key: a key to associate with the message, optional
        :type key: str
        :param headers: headers to associate with the message, optional
        :type headers: List[Tuple[str, bytes]]
        :param partition: the topic partition to send the message, optional
        :type partition: int
        :param timestamp_ms: a timestamp to associate with the message, optional
        :type timestamp_ms: int
        :return: a future with record metadata attached
        :rtype: FutureRecordMetadata
        """
        if value is None and key is None:
            raise ValueError("Either value or key must be set")

        # assert self.producer is not None, "producer was never set to a KafkaProducer"
        if producer is None:
            assert self._producers, "no producers were saved"
            producer = choice(list(self._producers))

        _add_cb = False
        if callbacks is not None:
            _add_cb = True
            callback = KafkaSendCallback()

            for cb in callbacks:
                callback.add_callback(cb)

            if flush:

                def flush_callback(f, producer):
                    producer.flush()
                    self.get_logger().info(f"Flushed producer {producer!r}")

                callback.add_callback(flush_callback, producer)

        _add_errback = errbacks is not None
        if errbacks is not None:
            _add_errback = True
            errback = KafkaSendCallback()
            for cb in errbacks:
                errback.add_callback(cb)

        f: FutureRecordMetadata

        if not _add_cb and not _add_errback:

            f = producer.send(
                topic,
                value=value,
                key=key,
                headers=headers,
                partition=partition,
                timestamp_ms=timestamp_ms,
            )

        elif _add_cb and _add_errback:

            f = (
                producer.send(
                    topic,
                    value=value,
                    key=key,
                    headers=headers,
                    partition=partition,
                    timestamp_ms=timestamp_ms,
                )
                .add_callback(callback)
                .add_errback(errback)
            )

        elif _add_cb:

            f = producer.send(
                topic,
                value=value,
                key=key,
                headers=headers,
                partition=partition,
                timestamp_ms=timestamp_ms,
            ).add_callback(callback)

        else:

            f = producer.send(
                topic,
                value=value,
                key=key,
                headers=headers,
                partition=partition,
                timestamp_ms=timestamp_ms,
            ).add_errback(errback)

        return _SendResult(producer, f)

    send = publish

    def send_timeout_ms(self, producer: Optional[KafkaProducer] = None):
        """
        Get and return the max blocking time went sending a kafka message

        If a producer is not passed, this function will collect the
        currently default arguments.

        Note: defaulk kafka arguments can be set in and are collected
            by multiple levels of configuartion.
            # TODO get link for heirarchy of souurces for kafka configuration collection

        :param producer: the KafkaProducer client, defaults to None
        :type producer: KafkaProducer, optional

        :returns: the max blocking time
        :rtype: int
        """
        if producer is not None:
            return producer.config["max_block_ms"]
        else:
            return self._producer_configs.get("max_block_ms")

    @staticmethod
    def _listen_sync_wrapper(*args, **kwargs):
        """
        Wwrapper to effectively run the `listen()` coroutine
        in an event loop in a `miltiprocessing.Process`, with
        proper exception handling and cleanup.

        This is a staticmethod because, otherwise, multiprocessing will
        complain that elements cached within `self` will not be picklable;
        like lambda functions with KafkaConsumer, a function effectively
        defined at runtikme and bound to a specific `KafkaConsumer`
        instance.
        """

        # Reimport necessary modules and types in the new process
        from queue import Empty  # noqa: F811
        from typing import TYPE_CHECKING, Set, Tuple

        from kafka import KafkaConsumer

        from .utils import get_default_consumer_configs

        # Import any other relevant objects (e.g., KafkaConsumerConfigs)
        if TYPE_CHECKING:
            from logging import Logger, LoggerAdapter  # noqa: F811
            from multiprocessing import Queue
            from multiprocessing.synchronize import Event

        shutdown_event: "Event" = kwargs['mpevent']
        poll_timeout_ms: int = int(kwargs.get('poll_timeout_ms', 1000))
        _topics: Set[str] = set(kwargs["topics"])
        api_version: Tuple[int, int, int] = kwargs["api_version"]
        consumer_configs = get_default_consumer_configs(api_version=api_version)
        consumer: KafkaConsumer = KafkaConsumer(*_topics, **consumer_configs)
        message_queue: "Queue" = kwargs["mpqueue"]
        control_queue: "Queue" = kwargs["control_queue"]
        logger: Union["Logger", "LoggerAdapter"] = kwargs["logger"]

        try:
            while not shutdown_event.is_set():  # Check for shutdown signal

                while not control_queue.empty():
                    try:
                        control_command = control_queue.get_nowait()
                    # although we check that control queue isn't empty upfront,
                    # we make sure we handle Empty errors should they be thrown
                    except Empty:
                        logger.warning(
                            "Race condition detected with retrieval of content from "
                            "the control_command queue...We check to see if the "
                            "queue is empty a few lines above, but it's found to be "
                            " empty after checking a second time."
                        )
                        break  # break from the `while not control_queue.empty()` loop
                    else:
                        if control_command.get("action") and control_command.get(
                            "topics"
                        ):

                            # Process the control command

                            # `consumer.topics()`` returns a set of topics of which
                            # it is aware, but not necessarily those to which it is
                            # currently subscribed
                            current_topics = consumer.subscription() or set()

                            match control_command:
                                case {"action": "subscribe", "topics": new_topics}:
                                    logger.info(
                                        f"Subscribing to new topics: {new_topics}"
                                    )
                                    updated_topics = current_topics.union(
                                        set(new_topics)
                                    )
                                    consumer.subscribe(updated_topics)
                                    logger.info(
                                        f"Consumer confirmed subscription: {consumer.subscription()}"
                                    )

                                case {"action": "unsubscribe", "topics": old_topics}:
                                    logger.info(
                                        f"Unsubscribing from topics: {old_topics}"
                                    )
                                    updated_topics = current_topics - set(old_topics)
                                    consumer.subscribe(updated_topics)
                                    logger.info(
                                        f"Consumer confirmed subscription: {consumer.subscription()}"
                                    )

                                case _:
                                    logger.warning(
                                        f"Received an unknown control command: {control_command}"
                                    )
                        else:
                            logger.warning(
                                f"Received an unknown control command: {control_command}"
                            )

                try:
                    # Poll for messages with a timeout to avoid blocking indefinitely
                    message_batch = consumer.poll(timeout_ms=poll_timeout_ms)

                    # Put each message into the multiprocessing queue
                    for tp, messages in message_batch.items():
                        for message in messages:
                            message_queue.put(message)  # Non-blocking queue put

                except Exception as e:
                    logger.error(
                        f"Error in Kafka consumer poll: {e}, "
                        f"current assignment: {consumer.assignment()}",
                        exc_info=True,
                    )

        finally:
            # Ensure the consumer and queue are properly closed when terminating
            consumer.close()
            logger.info("Kafka consumer process terminated and resources cleaned up.")

    def _prepare_multiprocess_context(self):
        self._is_multiprocess = True
        self.mpqueue = multiprocessing.Queue()
        self.mpevent = multiprocessing.synchronize.Event(
            ctx=multiprocessing.context._default_context
        )
        self.control_queue = multiprocessing.Queue()

    def start_listener_process(self, consumer: KafkaConsumer, *args, **kwargs):

        assert (
            not consumer._closed
        ), "Cannot start a listener process on a closed consumer"

        if (
            listener_process := self._consumer_listener_process_map[consumer]
        ) is not None:
            assert (
                not listener_process.is_alive()
            ), "A listener process on the passed consumer has already been opened"
            self._consumer_listener_process_map.pop(consumer)

        if (listener_task := self._consumer_listener_task_map[consumer]) is not None:
            if not listener_task.done():
                listener_task.cancel()

        self._prepare_multiprocess_context()

        kwargs["mpqueue"] = self.mpqueue
        kwargs["control_queue"] = self.control_queue
        kwargs["mpevent"] = self.mpevent
        kwargs["api_version"] = self._api_version
        kwargs["topics"] = self._subscription_handler._get_current_subscriptions(
            consumer
        )
        kwargs["logger"] = self.get_logger()

        listener_process = multiprocessing.Process(
            target=self._listen_sync_wrapper, args=args, kwargs=kwargs
        )
        listener_process.start()
        self._consumer_listener_process_map[consumer] = listener_process

        if (
            not hasattr(self, "mpqueue_listener_task")
            or self.mpqueue_listener_task.done()
        ):
            loop = self._ensure_loop()
            self.mpqueue_listener_task = loop.create_task(self.mpqueue_listener())

    def _stop_listener_processes(self):
        assert self._is_multiprocess, "no processes spawned"
        assert self.mpevent is not None, "no multiprocessing Event set"
        if not self.mpevent.is_set():
            self.mpevent.set()

        for listener_process in list(self._consumer_listener_process_map.values()):
            if listener_process is not None and listener_process.is_alive():
                listener_process.join()
                listener_process.close()

        if not self.mpqueue_listener_task.done():
            self.mpqueue_listener_task.cancel()

        assert self.mpqueue is not None

        self.mpqueue.join_thread()
        self.mpqueue.close()

        self.control_queue.join_thread()
        self.control_queue.close()

        self.get_logger().info("Listener process and queues stopped gracefully.")

    async def _close_all_waiter(self):

        self.get_logger().info("Started _close_all_waiter")
        await self._stop_event.wait()

        if self._is_multiprocess:
            self._stop_listener_processes()

        self.get_logger().info("Stop event set")

        for consumer in self._consumers:

            if (
                listener_task := self._consumer_listener_task_map[consumer]
            ) is not None and not listener_task.done():
                listener_task.cancel()

            if not consumer._closed:
                self.get_logger().info(f"Closing consumer {consumer!r}")
                consumer.close()

        for producer in self._producers:

            if not producer._closed:
                self.get_logger().info(f"Closing producer {producer!r}")
                producer.flush()
                producer.close()

        if hasattr(self, "_admin_client") and not self._admin_client._closed:
            self._admin_client.close()

        self.get_logger().info("Exiting _close_all_waiter")

    def stop(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
