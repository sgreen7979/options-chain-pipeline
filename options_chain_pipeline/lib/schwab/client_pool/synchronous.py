#!/usr/bin/env python3
import atexit
from contextlib import contextmanager
import datetime as dt
import threading
import time
from typing import Callable, List, Optional, TYPE_CHECKING

from daily.utils.logging import add_new_fh

# from ..client.meta import SchwabClientMeta
from daily.utils.singleton import StrictSingleton
from ..producer.client.synchronous import SyncSchwabProducer
from .base import ClientPool

if TYPE_CHECKING:
    from ..requestlib import MakeRequestModel


class SyncClientPool(
    ClientPool[SyncSchwabProducer],
    client_type=SyncSchwabProducer,
    metaclass=StrictSingleton,
):
    def __init__(self, clients: List[SyncSchwabProducer] | None = None) -> None:
        super().__init__(clients)
        self._client_threads: List[threading.Thread] = []
        self.closing = self.started = self.stopped = False
        self.get_logger().debug(f"Initializing {type(self).__name__}")

    def _await_client_with_capacity(self, *args, **kwargs):
        wait_times = self._get_wait_times()
        min_wait_time = min(wait_times)
        if min_wait_time > 0:
            time.sleep(min_wait_time)
        client = self._clients[wait_times.index(min_wait_time)]
        self.get_logger().debug(f"Acquired client {client.idx}")
        return client

    def _get_wait_times(self) -> List[float]:
        return [client.get_wait_time() for client in self._clients]

    def _get_next_dts(self) -> List[dt.datetime]:
        return [client.get_next_datetime() for client in self._clients]

    def _get_next_ts(self) -> List[float]:
        return [client.get_next_timestamp() for client in self._clients]

    def _get_next_client_with_capacity(self):
        """Returns the client with the earliest next capacity time

        NOTE: the client returned doesn't necessarily have capacity currently
        """
        wait_times = self._get_wait_times()
        min_wait_time = min(wait_times)
        client = self._clients[wait_times.index(min_wait_time)]
        return client

    @contextmanager
    def yield_client(
        # self, incl_queued: bool = False, freeze: bool = False
        self,
        urgent: bool = True,
        # ) -> AsyncGenerator["SchwabClient", Any, None]:
    ):
        """Yield the next client with capacity

        :param urgent: suspend processing of queued requests, defaults to True
        :type urgent: bool, optional

        .. REMOVED:
            :param incl_queued: consider queued requests when determining
                                the next client with capacity
                                defaults to False
            :type incl_queued: bool, optional
            :param freeze: temporarily freeze processing of queued requests
                                defaults to False
            :type freeze: bool, optional

        """
        with self.__class__._acquire_lock():
            wait_times = self._get_wait_times()
            min_wait_time = min(wait_times)
            time.sleep(min_wait_time)
            client = self._clients[wait_times.index(min_wait_time)]

            exit_funcs = []
            client._lock.acquire()
            exit_funcs.append(client._lock.release)

            if urgent:
                while client.event.is_set():
                    time.sleep(0.1)
                client.event.set()
                exit_funcs.append(client.event.clear)

            try:
                yield client
            except Exception as e:
                client.get_logger_adapter().error(
                    f"Client operation failed {e}", exc_info=True
                )
            finally:
                for exit_func in reversed(exit_funcs):
                    exit_func()

    def no_current_capacity(self) -> bool:
        return all([not c.has_capacity() for c in self._clients])

    def get_capacity_used(self) -> int:
        return sum([c.get_capacity_used() for c in self._clients])

    def get_capacity_queued(self) -> int:
        return sum([c.get_capacity_queued() for c in self._clients])

    def get_capacity_scheduled(self) -> int:
        return sum([c.get_capacity_scheduled() for c in self._clients])

    def get_current_capacity(self) -> int:
        return sum([c.get_current_capacity() for c in self._clients])

    def has_capacity(self) -> bool:
        return any([c.has_capacity() for c in self._clients])

    def get_capacity_summary(self, incl_queued: bool = True):
        return {
            c.idx: {
                "capacity_used": c.get_capacity_used(),
                "capacity_scheduled": c.get_capacity_scheduled(),
                "capacity_queued": (0 if not incl_queued else c.get_capacity_queued()),
                "next_availability": (c.get_next_datetime()).isoformat(),
            }
            for c in self._clients
        }

    def get_request_log(self):
        from ..client.functions import get_request_log_sync

        return get_request_log_sync(self._clients)

    @property
    def rolling_capacity(self) -> int:
        return sum(c.rolling_capacity for c in self._clients)

    def start(
        self,
        callback: Optional[Callable] = None,
        endpoint: Optional[str] = None,
        join: bool = False,
    ) -> List[threading.Thread]:
        self.__class__._create_lock_file()
        self.start_time = time.time()
        self.started = True

        add_new_fh(self.get_logger(), fh_level="DEBUG")
        self.get_logger().info("Starting")
        atexit.register(self.get_logger().info, "Exiting")

        threading.Thread(target=self.log_updates).start()

        for client in self._clients:
            if callback is not None and endpoint is not None:
                client.set_callback(callback, endpoint)
            client_thread = client.start(join=join)
            self._client_threads.append(client_thread)

        return self.client_threads

    def enqueue_make_request(
        self,
        make_request: "MakeRequestModel",
        client: Optional[SyncSchwabProducer] = None,
    ):
        # FIXNE add this import to requestlib/__init__.py
        from ..requestlib.task import SyncRequestTaskModel

        # fmt: off
        return (
            SyncRequestTaskModel(make_request=make_request)
            .assign((client or self.next_client()).idx)
            .save()
            .stage(mode="uuid")
        )
        # fmt: on

    def clear_queues(self):
        for c in self._clients:
            c.queue.clear()
