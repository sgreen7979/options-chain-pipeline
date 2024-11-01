from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from kafka import KafkaProducer
    from kafka.producer.kafka import FutureRecordMetadata


class Node:

    __slots__ = ("_func", "_args", "_kwargs", "_next")

    def __init__(self, func: Callable, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._next = None

    @property
    def next(self):
        return self._next

    def set_next(self, next: "Node"):
        self._next = next

    def value(self, f):
        self._func(f, *self._args, self._kwargs)
        if self._next:
            self._next.value(f)

    @classmethod
    def with_next(cls, next: "Node", func: Callable, *args, **kwargs):
        inst = cls(func, *args, **kwargs)
        inst.set_next(next)
        return inst


class KafkaSendCallback:

    def __init__(
        self,
        # *callbacks: Callable,
        # args: list[tuple[object, ...]] | None = None,
        # kwargs: list[dict[str, object]] | None = None,
    ):
        # self._callbacks = callbacks
        self._callbacks = []
        # self._args = args or [() for _ in callbacks]
        # self._args = []
        # self._kwargs = kwargs or [{} for _ in callbacks]
        # self._kwargs = []
        # self._head: Node
        # self._tail: Node
        # self._accumulate()

    # def _accumulate(self):
    #     tail = head = Node(self._callbacks[0], *self._args[0], **self._kwargs[0])
    #     for cb in iter(self._callbacks[1:]):
    #         node = Node(cb)
    #         tail.set_next(node)
    #         tail = node
    #     self._tail, self._head = tail, head

    def add_callback(self, cb: Callable, *args, **kwargs):
        node = Node(cb, *args, **kwargs)
        if not hasattr(self, "_head"):
            self._tail = self._head = node
        else:
            tail = self._tail
            tail.set_next(node)
            self._tail = node

    def __call__(self, f):
        return self._head.value(f)


class _SendResult:
    """
    Class that houses the producer that sent the message
    and the FutureRecordMetadata returned by `producer.send`

    Usage:
        ```
        service = KafkaService()
        producer = service.initialize_producer()
        result: _SendResult = service.publish("foo-topic", b"foo-message")
        result._f._produce_success(timeout=15.0)
        result.flush()

        service.publish("foo-topic", b"foo-message", producer=producer).flush()

        service.publish("foo-topic", b"foo-message").flush()

        ...
        ```
    """

    __slots__ = ("_producer", "_f")

    def __init__(self, producer: "KafkaProducer", f: "FutureRecordMetadata"):
        self._producer: "KafkaProducer" = producer
        self._f: "FutureRecordMetadata" = f

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        :param timeout: timeout in seconds to wait for completion, defaults to None
        :type timeout: float, optional
        :return: nothing
        :rtype: None
        """
        self._producer.flush(timeout)
