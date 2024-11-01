#!/usr/bin/env python3
import asyncio
import atexit
from contextlib import asynccontextmanager
import datetime as dt
import json
import threading
import time
from typing import TYPE_CHECKING, Callable, List, Optional

from daily.utils.logging import add_new_fh
from daily.utils.singleton import StrictSingleton

from ..producer.client.asynchronous import _AsyncSchwabProducer, AsyncSchwabProducer
from .base import ClientPool

if TYPE_CHECKING:
    from ..requestlib import MakeRequestModel


class AsyncClientPool(
    ClientPool[AsyncSchwabProducer],
    client_type=AsyncSchwabProducer,
    metaclass=StrictSingleton,
):
    def __init__(self, clients: Optional[List[AsyncSchwabProducer]] = None) -> None:
        if clients is None:
            from ..client.functions import get_all_clients

            clients = get_all_clients(client_factory=AsyncSchwabProducer, login=False)

        super().__init__(clients)
        self._client_threads: List[threading.Thread] = []
        self.closing = self.started = self.stopped = self._loop_set = False
        self.get_logger().debug(f"Initializing {type(self).__name__}")

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        if not hasattr(self, "loop") or self.loop.is_closed():
            self.loop = loop
            for c in self._clients:
                c.set_loop(loop)
            self._loop_set = True

    async def _await_client_with_capacity(self, *args, **kwargs):
        wait_times = await self._get_wait_times()
        min_wait_time = min(wait_times)
        if min_wait_time > 0:
            await asyncio.sleep(min_wait_time)
        client = self._clients[wait_times.index(min_wait_time)]
        self.get_logger().debug(f"Acquired client {client.idx}")
        return client

    async def _get_wait_times(self) -> List[float]:
        return [await client.get_wait_time() for client in self._clients]

    async def _get_next_dts(self) -> List[dt.datetime]:
        return [await client.get_next_datetime() for client in self._clients]

    async def _get_next_ts(self) -> List[float]:
        return [await client.get_next_timestamp() for client in self._clients]

    async def _get_next_client_with_capacity(self):
        """Returns the client with the earliest next capacity time

        NOTE: the client returned doesn't necessarily have capacity currently
        """
        wait_times = await self._get_wait_times()
        min_wait_time = min(wait_times)
        client = self._clients[wait_times.index(min_wait_time)]
        return client

    @asynccontextmanager
    async def yield_client(
        # self, incl_queued: bool = False, freeze: bool = False
        self,
        urgent: bool = True,
        # ) -> AsyncGenerator["SchwabClient", Any, None]:
    ):
        """Yield the next client with capacity

        :param urgent: suspend processing of queued requests, defaults to True
        :type urgent: bool, optional


        ..REMOVED::

            :param incl_queued: consider queued requests when determining
                                the next client with capacity
                                defaults to False
            :type incl_queued: bool, optional
            :param freeze: temporarily freeze processing of queued requests
                           defaults to False
            :type freeze: bool, optional

        """
        with self.__class__._acquire_lock():
            wait_times = await self._get_wait_times()
            min_wait_time = min(wait_times)
            await asyncio.sleep(min_wait_time)
            client = self._clients[wait_times.index(min_wait_time)]

            exit_funcs = []
            await client._lock.acquire()
            exit_funcs.append(client._lock.release)

            if urgent:
                while client._pause_event.is_set():
                    await asyncio.sleep(0.1)
                client._pause_event.set()
                exit_funcs.append(client._pause_event.clear)

            try:
                yield client
            except Exception as e:
                client.get_logger_adapter().error(
                    f"Client operation failed {e}", exc_info=True
                )
            finally:
                for exit_func in reversed(exit_funcs):
                    exit_func()

    async def no_current_capacity(self) -> bool:
        return all([not await c.has_capacity() for c in self._clients])

    async def get_capacity_used(self) -> int:
        return sum([await c.get_capacity_used() for c in self._clients])

    async def get_capacity_queued(self) -> int:
        return sum([await c.get_capacity_queued() for c in self._clients])

    async def get_capacity_scheduled(self) -> int:
        return sum([await c.get_capacity_scheduled() for c in self._clients])

    async def get_current_capacity(self) -> int:
        return sum([await c.get_current_capacity() for c in self._clients])

    async def has_capacity(self) -> bool:
        return any([await c.has_capacity() for c in self._clients])

    async def get_capacity_summary(self, incl_queued: bool = True):
        return {
            c.idx: {
                "capacity_used": await c.get_capacity_used(),
                "capacity_scheduled": await c.get_capacity_scheduled(),
                "capacity_queued": await c.get_capacity_queued(),
                "next_availability": (await c.get_next_datetime()).isoformat(),
            }
            for c in self._clients
        }

    async def get_request_log(self):
        from ..client.functions import get_request_log_async

        return await get_request_log_async(self._clients)

    @property
    async def rolling_capacity(self) -> int:
        return sum([await c.rolling_capacity for c in self._clients])

    def _rolling_capacity(self) -> int:
        return sum(c._repo_service._rolling_capacity for c in self._clients)

    def start(
        self,
        callback: Optional[Callable] = None,
        endpoint: Optional[str] = None,
        join: bool = False,
    ) -> None:
        self.__class__._create_lock_file()
        self.start_time = time.time()
        self.started = True

        add_new_fh(self.get_logger(), fh_level="DEBUG")
        self.get_logger().info("Starting")
        atexit.register(self.get_logger().info, "Exiting")

        asyncio.create_task(self.log_updates())

        for client in self._clients:
            if callback is not None and endpoint is not None:
                client.set_callback(callback, endpoint)
            client.start(join=join)

    @property
    def done(self) -> bool:
        return all(c.done for c in self._clients)

    async def __anext__(self):
        await asyncio.sleep(0.5)
        return await self._await_client_with_capacity()

    def __aiter__(self):
        return self

    async def enqueue_make_request(
        self,
        make_request: "MakeRequestModel",
        client: Optional[AsyncSchwabProducer] = None,
    ):
        from ..requestlib.task import AsyncRequestTaskModel

        # fmt: off
        task = (
            AsyncRequestTaskModel(make_request=make_request)  # type: ignore
            .assign((client or self.next_client()).idx)
        )
        await task.save()
        await task.stage(mode="uuid")
        return task

    async def clear_queues(self):

        class _AsyncContainer:

            __slots__ = ("_items", "_cursor")

            def __init__(self, *items):
                self._items = items
                self._cursor = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._cursor >= len(self._items):
                    raise StopAsyncIteration
                next_item = self._items[self._cursor]
                self._cursor += 1
                return next_item

        async for c in _AsyncContainer(*self._clients):
            await c.queue.clear()

    async def _log_async_capacity_updates(self, *, timeout_ms: int = 15000):
        """Log async capacity summaries of the underlying clients.

        :param timeout_ms: timeout in milliseconds between each update,
            defaults to 15000 (15 seconds)
        :type timeout_ms: int, optional

        """
        assert timeout_ms >= 0, f"timeout_ms ({timeout_ms}) is less than 0"
        while not self.closing:
            await asyncio.sleep(timeout_ms / 1000)
            data = await self.get_capacity_summary()
            self.get_logger().info(json.dumps(data, indent=4))

    async def log_updates(self, *, timeout_ms: int = 15000):
        await self._log_async_capacity_updates(timeout_ms=timeout_ms)

    async def reserve_capacity(self, client_idx: int, capacity_to_reserve: int):
        pass
