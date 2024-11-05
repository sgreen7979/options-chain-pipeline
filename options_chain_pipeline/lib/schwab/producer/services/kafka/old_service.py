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
    Concatenate,
    DefaultDict,
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

from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.producer.kafka import FutureRecordMetadata

from daily.types import StrPath
from daily.utils.logging import ClassNameLoggerMixin

from .models import ControlCommand, KafkaConsumerConfigs, KafkaProducerConfigs
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


class KafkaService(ClassNameLoggerMixin):
    """
    This class is designed to provide a user-friendly
    interface that implements the consumer, producer,
    handler model and central configuration handling
    for all KafkaClient variants.

    Features of this class include:

        *Multiprocessing via `start_listener_process`*
        *Async iteration via `listen` when not multiprocessing*
    """

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

    def __init__(
        self,
        bootstrap_servers: Optional[Union[str, List[str]]] = None,
        *,
        api_version: Optional[Tuple[int, int, int]] = None,
        config_file: Optional[StrPath] = None,
    ) -> None:
        self._bootstrap_servers: Union[str, List[str]] = (
            bootstrap_servers or self.DEFAULT_BOOTSTRAP_SERVERS
        )
        self._api_version: Tuple[int, int, int] = (
            api_version or self.DEFAULT_BROKER_API_VERSION
        )

        self._config_file: Optional[StrPath] = config_file

        self._consumer_configs: KafkaConsumerConfigs = get_default_consumer_configs(
            self._bootstrap_servers, self._api_version
        ).copy()
        self._producer_configs: KafkaProducerConfigs = get_default_producer_configs(
            self._bootstrap_servers, self._api_version
        ).copy()

        self._handlers: List[KafkaHandler] = []
        self._topics: Set[str] = set()

        # self._consumer: Optional[KafkaConsumer] = None
        self._consumers: Set[KafkaConsumer] = set()
        # self._producer: Optional[KafkaProducer] = None
        self._producers: Set[KafkaProducer] = set()

        self._consumer_assignment_map: DefaultDict[KafkaConsumer, Set[str]] = (
            defaultdict(set)
        )
        self._consumer_listener_task_map: DefaultDict[KafkaConsumer, asyncio.Task] = (
            defaultdict(None)
        )

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

    @property
    def consumer_assignment_map(self) -> DefaultDict[KafkaConsumer, Set[str]]:
        """A mapping of consumers to topics to which they are
        currently subscribed.

        :return: the consumer assignment map
        :rtype: DefaultDict[KafkaConsumer, Set[str]]
        """
        for consumer in self._consumers:
            self._consumer_assignment_map[consumer] = self._get_current_subscriptions(
                consumer
            )
        return self._consumer_assignment_map

    @staticmethod
    def _get_current_subscriptions(consumer: KafkaConsumer) -> Set[str]:
        """Returns a set of topics to which a given consumer
        is currently subscribed

        :return: the currently subscribed topics
        :rtype: Set[str]
        """
        return consumer.subscription() or set()

    def subscribed_to(self, topic: str) -> bool:
        """Whether any consumers are currently subscribed to
        a given topic

        :param topic: the topic
        :type topic: str
        :return: whether any consumers are currently subscribed to the topic
        :rtype: bool
        """
        for consumer in self._consumers:
            if topic in self._get_current_subscriptions(consumer):
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

    def load_config(self):
        if self._config_file is not None:
            # implement config loader
            pass

    def add_consumer(self, consumer: KafkaConsumer):
        if consumer in self._consumers:
            self.get_logger().warning(f"Consumer already saved {consumer!r}")
        with self._consumers_lock:
            self._consumers.add(consumer)

    def add_producer(self, producer: KafkaProducer):
        if producer in self._producers:
            self.get_logger().warning(f"Producer already saved {producer!r}")
        else:
            with self._producers_lock:
                self._producers.add(producer)

    def add_handler(self, handler: KafkaHandler):
        if handler not in self._handlers:
            with self._handlers_and_topics_lock:
                self._handlers.append(handler)
                self._topics.update(set(handler.topics))

            if self._is_multiprocess is True and not self.mpevent.is_set():
                self.control_queue.put(
                    ControlCommand(action="subscribe", topics=handler.topics)
                )
            else:
                for topic in handler.topics:
                    if not self.subscribed_to(topic):
                        self.get_logger().warning(
                            "No consumers are currently subscribed "
                            f"subscribed to topic {topic}"
                        )

    @overload
    def _get_configs(
        self,
        client_type: Type[KafkaConsumer],
        set_as_default: bool = False,
        **configs: Unpack[KafkaConsumerConfigs],
    ) -> KafkaConsumerConfigs: ...

    @overload
    def _get_configs(
        self,
        client_type: Type[KafkaProducer],
        set_as_default: bool = False,
        **configs: Unpack[KafkaProducerConfigs],
    ) -> KafkaProducerConfigs: ...

    def _get_configs(
        self,
        client_type: Union[Type[KafkaConsumer], Type[KafkaProducer]],
        set_as_default: bool = False,
        **configs,
    ) -> Union[KafkaConsumerConfigs, KafkaProducerConfigs]:
        """Gather and return the initialization configs
        for a given Kafka client_type.

        This function will first set the configs to a
        copy of the currently set configs attribute
        (`_consumer_configs` if client_type is
        `KafkaConsumer` and `_producer_configs` if
        client_type is `KafkaProducer`) and then update
        them for the configs passed.

        If `set_as_default` is True, we update the
        currently set configs attribute to reflect those
        generated by this invocation.

        :param client_type: the Kafka client type
        :type client_type: Union[Type[KafkaConsumer], Type[KafkaProducer]]
        :param set_as_default: save the generated configs
            for future use, defaults to False
        :type set_as_default: bool, optional
        :raises TypeError: if client_type is not KafkaConsumer or
            KafkaProducer
        :return: the configs
        :rtype: Union[KafkaConsumerConfigs, KafkaProducerConfigs]
        """
        if client_type is KafkaConsumer:
            _configs = self._consumer_configs.copy()
            _configs.update(cast(KafkaConsumerConfigs, configs))
            if set_as_default:
                self._consumer_configs = _configs.copy()
        elif client_type is KafkaProducer:
            _configs = self._producer_configs.copy()
            _configs.update(cast(KafkaProducerConfigs, configs))
            if set_as_default:
                self._producer_configs = _configs.copy()
        else:
            raise TypeError(
                f"Expected type KafkaConsumer or KafkaProducer, got {client_type!r}"
            )
        return _configs

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

    def _initialize_client(
        self,
        client_type: Union[Type[KafkaConsumer], Type[KafkaProducer]],
        set_as_default: bool = False,
        *topics: str,
        **configs,
    ) -> Union[KafkaConsumer, KafkaProducer]:
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
        _configs = self._get_configs(client_type, set_as_default, **configs)
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
        self._topics.update(topics)

        self._consumers.add(consumer)
        return consumer

    # @property
    # def consumer(self):
    #     return self._consumer

    # @property
    # def producer(self):
    #     return self._producer

    # def set_producer(self, producer: KafkaProducer) -> Self:
    #     self._producer = producer
    #     return self

    # def set_consumer(self, consumer: KafkaConsumer) -> Self:
    #     self._consumer = consumer
    #     return self

    async def fire_handlers(self):
        tasks = [handler.fire_callbacks() for handler in self._handlers]
        await asyncio.gather(*tasks)

    def _get_consumers_cycle(self):
        assert self._consumers, "no consumers were saved"
        return cycle(self._consumers)

    async def queue_listener(self):
        while not self._stop_event.is_set():
            try:
                # await self._queue.get()
                message = await asyncio.to_thread(self._queue.get)
                # message = self._queue.get_nowait()
            except Exception as e:
                self.get_logger().error(
                    f"Exception raised in queue_listener: {e}", exc_info=True
                )
            else:
                message = await message
                self.get_logger().info(
                    f"Retrieved message from the queue (topic={message.topic}, "
                    f"offset={message.offset})"
                )
                KAFKA_MESSAGE_VAR.set(message)
                await self.fire_handlers()
            finally:
                await asyncio.sleep(0.1)

        self.get_logger().info("queue_listener exiting")

    def _start_close_all_waiter(self):
        if not hasattr(self, "_close_all_task"):
            self._ensure_loop()
            self._close_all_task = self._loop.create_task(self._close_all_waiter())  # type: ignore[attr-defined]

    def start_queue_listener(self):

        if not self._queue_listener_started:

            self._ensure_loop()

            # task = asyncio.create_task(self.queue_listener())
            task = self._loop.create_task(self.queue_listener())  # type: ignore[attr-defined]
            self._queue_listener_task = task
            self.get_logger().info("Started queue_listener")
            self._start_close_all_waiter()
            return task

    async def _listen(self, consumer: KafkaConsumer):
        while not consumer._closed or not self._stop_event.is_set():
            message_batch = consumer.poll(
                timeout_ms=self._poll_timeout_ms, max_records=1
            )
            for tp, messages in message_batch.items():
                if messages:
                    message = cast(ConsumerRecord, messages[0])
                    self.get_logger().info(
                        f"Received message from {consumer!r}: "
                        f"(topic={message.topic}, offset={message.offset})"
                    )
                    await self._queue.put(message)
            await asyncio.sleep(0.1)

        self.get_logger().info(f"Exiting listen loop for conusmer {consumer!r}")

    def listen(self, consumer: KafkaConsumer):
        if self._consumer_listener_task_map[consumer] is None:
            self.start_queue_listener()
            # listener_task = asyncio.create_task(self._listen(consumer))
            self._ensure_loop()
            listener_task = self._loop.create_task(self._listen(consumer))  # type: ignore[attr-defined]
            self._consumer_listener_task_map[consumer] = listener_task

    def _ensure_loop(self):
        if self._loop is None or self._loop.is_closed():
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

    # async def _listen(self, *args, **kwargs):
    #     # assert self.consumer is not None, "consumer was never set to a KafkaConsumer"
    #     assert self._consumers, "no consumers saved"
    #     consumer_cycle = self._get_consumers_cycle()
    #     consumer = next(consumer_cycle)
    #     if consumer._closed:
    #         self.get_logger().error(f"Consumer is closed, removing it {consumer!r}")
    #         self._consumers.remove(consumer)
    #     else:
    #         # Poll for messages with a timeout to avoid blocking indefinitely
    #         message_batch = consumer.poll(
    #             timeout_ms=self._poll_timeout_ms, max_records=1
    #         )

    #         # Put each message into the multiprocessing queue
    #         for tp, messages in message_batch.items():
    #             if messages:
    #                 message = cast(ConsumerRecord, messages[0])
    #                 KAFKA_MESSAGE_VAR.set(message)
    #                 await self.fire_handlers()
    #                 await asyncio.sleep(0.2)

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
                    await asyncio.sleep(0.2)

                # message = await asyncio.to_thread(self.mpqueue.get, timeout=1)
                # # message = self.mpqueue.get()
                # KAFKA_MESSAGE_VAR.set(message)
                # await self.fire_handlers()
                # # loop = asyncio.get_running_loop()
                # # loop.create_task(self.fire_handlers())

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
        # assert self.consumer is not None, "consumer was never set to a KafkaConsumer"
        self._topics.update(topics)

        # `consumer.subscribe` is not incremental, meaning every time we
        # call it, the topics we pass to it will be the *only* topics
        # the consumer is actually subscribed to; so we pass it our aggregated set
        # self.consumer.subscribe(topics=self._topics, pattern=pattern, listener=listener)
        if self._is_multiprocess:
            self.control_queue.put({"action": "subscribe", "topics": set(topics)})
        else:
            subs = self._get_current_subscriptions(consumer)
            subs.update(topics)
            self._subscribe(consumer, *subs, pattern=pattern, listener=listener)

    def _subscribe(
        self, consumer: KafkaConsumer, *topics: str, pattern=None, listener=None
    ):
        # assert self.consumer is not None, "consumer was never set to a KafkaConsumer"
        # self.consumer.subscribe(topics, pattern=pattern, listener=listener)
        consumer.subscribe(topics, pattern=pattern, listener=listener)
        self.get_logger().info(
            f"Consumer now subscribed to {self._get_current_subscriptions(consumer)}"
        )

    def unsubscribe(self, consumer: KafkaConsumer, *topics: str):
        # self._topics -= set(topics)
        # self._subscribe(*self._topics)
        if self._is_multiprocess:
            self.control_queue.put({"action": "unsubscribe", "topics": set(topics)})
        else:
            subs = self._get_current_subscriptions(consumer)
            subs -= set(topics)
            self._subscribe(consumer, *subs)

    # ) -> FutureRecordMetadata:
    def publish(
        self,
        topic: str,
        value: Optional[Union[bytes, Any]] = None,
        key: Optional[str] = None,
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

        # import debugpy

        # debugpy.listen(("127.0.0.1", 7446))  # Use a different port for the subprocess
        # print("Waiting for debugger to attach in subprocess...")
        # debugpy.wait_for_client()  # The subprocess will wait for the debugger
        # print("Debugger attached to subprocess.")

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

            # from kafka.consumer.fetcher import ConsumerRecord

            # from .models import KafkaConsumerConfigs, ControlCommand

        shutdown_event: "Event" = kwargs['mpevent']
        poll_timeout_ms: int = int(kwargs.get('poll_timeout_ms', 1000))
        # consumer_configs: "KafkaConsumerConfigs" = kwargs["consumer_configs"]
        _topics: Set[str] = set(kwargs["topics"])
        api_version: Tuple[int, int, int] = kwargs["api_version"]
        consumer_configs = get_default_consumer_configs(api_version=api_version)
        # consumer_configs.setdefault("api_version", api_version)
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
                                    # updated_topics = current_topics.difference(
                                    #     old_topics
                                    # )
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
                    # logger.error(f"Error in Kafka consumer poll: {e}", exc_info=True)
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
        # # We shutdown the consumer before the multiprocessing.Process
        # # to evade any NoBrokerConnectionAvailable errors
        # if self.consumer is not None and not self.consumer._closed:
        #     self._consumer_configs = cast(KafkaConsumerConfigs, self.consumer.config)
        #     self.consumer.close()

        self._is_multiprocess = True
        # self.mpqueue = multiprocessing.Queue[ConsumerRecord]()
        self.mpqueue = multiprocessing.Queue()
        self.mpevent = multiprocessing.synchronize.Event(
            ctx=multiprocessing.context._default_context
        )
        # self.control_queue = multiprocessing.Queue[ControlCommand]()
        self.control_queue = multiprocessing.Queue()

    def start_listener_process(self, *args, **kwargs):
        self._prepare_multiprocess_context()

        kwargs["mpqueue"] = self.mpqueue
        kwargs["control_queue"] = self.control_queue
        kwargs["mpevent"] = self.mpevent
        # ensure consumer_configs passd to the process
        # are not None and reflect our currently set
        # bootstrap_servers
        # consumer_configs = self._consumer_configs or {}
        # consumer_configs.setdefault("bootstrap_servers", self._bootstrap_servers)
        kwargs["api_version"] = self._api_version
        # # kwargs["consumer_configs"] = consumer_configs
        # consumer_configs = deepcopy(self._consumer_configs)
        # del consumer_configs["partition_assignment_strategy"]
        # del consumer_configs["default_offset_commit_callback"]
        # if "selector" in consumer_configs:
        #     del consumer_configs["selector"]
        # # kwargs["consumer_configs"] = self._consumer_configs
        # import types
        # to_delete = []
        # for k, v in consumer_configs.items():
        #     if isinstance(v, types.LambdaType):
        #         print(k, v)
        #         to_delete.append(k)
        # for k in to_delete:
        #     del consumer_configs[k]
        # kwargs["consumer_configs"] = consumer_configs

        kwargs["topics"] = self._topics
        kwargs["logger"] = self.get_logger()

        self.listener_process = multiprocessing.Process(
            target=self._listen_sync_wrapper, args=args, kwargs=kwargs
        )
        self.listener_process.start()

        self._ensure_loop()
        # self.mpqueue_listener_task = asyncio.create_task(self.mpqueue_listener())
        self.mpqueue_listener_task = self._loop.create_task(self.mpqueue_listener())  # type: ignore[attr-defined]

    def _stop_listener_process(self):
        assert self._is_multiprocess, "no processes spawned"
        if self.listener_process.is_alive():
            assert self.mpevent is not None
            if not self.mpevent.is_set():
                self.mpevent.set()

            self.listener_process.join()
            self.listener_process.close()

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
            self._stop_listener_process()

        self.get_logger().info("Stop event set")
        for consumer in self._consumers:
            if (
                t := self._consumer_listener_task_map[consumer]
            ) is not None and not t.done():
                t.cancel()
            if not consumer._closed:

                self.get_logger().info(f"Closing consumer {consumer!r}")
                consumer.close()
        for producer in self._producers:
            if not producer._closed:
                self.get_logger().info(f"Closing producer {producer!r}")
                producer.flush()
                producer.close()
        self.get_logger().info("Exiting _close_all_waiter")

    def stop(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
