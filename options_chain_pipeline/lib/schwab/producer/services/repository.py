from abc import abstractmethod, ABC
import asyncio
from contextlib import contextmanager, asynccontextmanager
import datetime as dt
from functools import cached_property
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    ClassVar,
    Generic,
    Generator,
    List,
    Optional,
    Self,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import redis
import redis.asyncio as aioredis

from daily.utils.classproperty import classproperty
from daily.utils.redis import (
    RedisConnectionManager,
    SyncRedisExecuted,
    SyncRedisQueue,
    SyncRedisRequestTasks,
    SyncRedisScheduled,
    AsyncRedisExecuted,
    AsyncRedisQueue,
    AsyncRedisRequestTasks,
    AsyncRedisScheduled,
    RedisKeys,
)
from daily.utils.rolling_window import RollingWindow

from .client.asynchronous import AsyncSchwabProducer
from .client.synchronous import SyncSchwabProducer

_T = TypeVar('_T')
_R = TypeVar("_R", redis.Redis, aioredis.Redis)
_C = TypeVar("_C", AsyncSchwabProducer, SyncSchwabProducer)
RequestTimesT = TypeVar('RequestTimesT', List[float], Tuple[List[float], List[float]])


class BaseRepositoryService(Generic[_C], ABC):

    Manager: ClassVar[RedisConnectionManager] = RedisConnectionManager()
    rolling_window_sec: ClassVar[float] = 60.0

    @classproperty
    def rolling_window(cls):
        return RollingWindow(cls.rolling_window_sec)

    @classmethod
    def set_rolling_window_sec(cls, secs: float):
        assert secs > 0
        cls.rolling_window_sec = secs

    def __init__(
        self,
        client: _C,
        rolling_capacity: Optional[int] = None,
        scheduling_capacity: Optional[int] = None,
        queue_capacity: Optional[int] = None,
    ):
        self._client = client
        self._scheduling_capacity: int = (
            scheduling_capacity or client.config["rolling_sixty_limit"]
        )
        self._rolling_capacity: int = (
            rolling_capacity or client.config["rolling_sixty_limit"]
        )
        self._queue_capacity = queue_capacity or 500

    @cached_property
    def idx(self):
        return self._client.idx

    def get_logger_adapter(self):
        return self._client.get_logger_adapter()

    @cached_property
    def redis_keys(self):
        return RedisKeys(self.idx)

    @property
    def rolling_capacity(self) -> int:
        return self._rolling_capacity - self.reserved

    @property
    def scheduling_capacity(self) -> int:
        return self._scheduling_capacity - self.reserved

    @property
    def queue_capacity(self) -> int:
        return self._queue_capacity

    def set_scheduling_capacity(self, n: int) -> Self:
        assert n > 0
        self._scheduling_capacity = n
        self.get_logger_adapter().info(f"Set scheduling capacity {n}")
        return self

    def set_rolling_capacity(self, n: int) -> Self:
        assert n > 0
        self._rolling_capacity = n
        self.get_logger_adapter().info(f"Set rolling capacity {n}")
        return self

    def set_queue_capacity(self, n: int) -> Self:
        assert n > 0
        self._queue_capacity = n
        self.get_logger_adapter().info(f"Set queue capacity {n}")
        return self

    def set_redis_key_suffix(self, suffix) -> Self:
        self.redis_keys.set_suffix(suffix)
        self.redis_key_suffix = suffix
        self.get_logger_adapter().debug(f"Set redis key suffix {suffix}")
        return self

    # @abstractmethod
    # def get_executed_request_times(self) -> List[float]: ...

    # def get_executed_request_times(self) -> List[float]:
    def get_executed_request_times(self):
        """Returns a list of timestamps of requests executed
        in the last 60.0 seconds
        """
        return self.executed.sorted()

    # @abstractmethod
    # def get_scheduled_request_times(self) -> List[float]: ...

    def get_scheduled_request_times(self):
        """
        Returns a list of timestamps of requests scheduled
        in the future
        """
        return self.sched.sorted()

    @abstractmethod
    def get_all_request_times(self, type: Type[RequestTimesT]) -> RequestTimesT:
        """Get the current executed and scheduled request times.

        If type equals List[float], this function returns
        a sorted list combining the executed and scheduled
        request times.

        If type equals Tuple[List[float], List[float]], this
        function returns a tuple containing a sorted list of
        the executed request times and a sorted list of the
        scheduled request times.
        """
        ...

    @abstractmethod
    def get_all_next_times(self) -> List[float]:
        """
        Returns a combined list of the current executed and
        scheduled request times, except each time contained
        is increased by the rolling window (generally 60
        seconds).

        This function is meant to be a helper function for
        ascertaining times to schedule future requests.
        """
        ...

    @abstractmethod
    def has_available_executed_capacity(self) -> bool: ...

    @abstractmethod
    def has_available_scheduling_capacity(self) -> bool: ...

    @abstractmethod
    def get_wait_time(self) -> float:
        """
        Return the time in seconds until the client has
        available request (executed) capacity.
        """
        ...

    # @abstractmethod
    # def get_next_timestamp(self) -> float: ...

    def get_next_timestamp(self) -> float:
        """Returns the timestamp of when the client will next have capacity"""
        return self._get_next_capacity_timestamp()

    @abstractmethod
    def get_next_datetime(self) -> "dt.datetime": ...

    @abstractmethod
    def _get_next_capacity_timestamp(self) -> float: ...

    def get_available_executed_capacity(self):
        if asyncio.iscoroutinefunction(self.get_capacity_executed):

            async def _get_available_executed_capacity() -> int:

                if TYPE_CHECKING:
                    assert asyncio.iscoroutinefunction(self.get_capacity_executed)

                return self.rolling_capacity - await self.get_capacity_executed()

            return _get_available_executed_capacity()
        else:
            capacity_executed = self.get_capacity_executed()
            if TYPE_CHECKING:
                assert isinstance(capacity_executed, int)
            return self.rolling_capacity - capacity_executed

    def get_available_scheduling_capacity(self):
        if asyncio.iscoroutinefunction(self.get_capacity_scheduled):

            async def _get_available_scheduling_capacity() -> int:

                if TYPE_CHECKING:
                    assert asyncio.iscoroutinefunction(self.get_capacity_scheduled)

                return self.scheduling_capacity - await self.get_capacity_scheduled()

            return _get_available_scheduling_capacity()
        else:
            capacity_scheduled = self.get_capacity_scheduled()
            if TYPE_CHECKING:
                assert isinstance(capacity_scheduled, int)
            return self.scheduling_capacity - capacity_scheduled

    @abstractmethod
    def get_available_capacities(self) -> Tuple[int, int]: ...

    @abstractmethod
    def full_executed_capacity_available(self) -> bool: ...

    def get_capacity_executed(self):
        return self.executed.get_length()

    def get_capacity_scheduled(self):
        return self.sched.get_length()

    def get_capacity_queued(self):
        return self.queue.qsize()

    @abstractmethod
    def get_available_queue_capacity(self) -> int: ...

    @abstractmethod
    def has_available_queue_capacity(self) -> bool: ...

    @abstractmethod
    def _get_trailing_times(self) -> List[float]: ...

    @abstractmethod
    def _get_next_scheduling_times(self, n: int) -> List[float]: ...

    def _get_redis_helper(self, key: str, redis_helper_type: Type[_T], **kwargs) -> _T:
        return redis_helper_type(key, **kwargs)  # type: ignore

    @property
    @abstractmethod
    def queue(self) -> Union[SyncRedisQueue, AsyncRedisQueue]: ...

    @property
    @abstractmethod
    def sched(self) -> Union[SyncRedisScheduled, AsyncRedisScheduled]: ...

    @property
    @abstractmethod
    def executed(self) -> Union[SyncRedisExecuted, AsyncRedisExecuted]: ...

    @property
    @abstractmethod
    def task_db(self) -> Union[SyncRedisQueue, AsyncRedisQueue]: ...

    @abstractmethod
    def _await_capacity(self) -> _C: ...

    @abstractmethod
    def get_capacity_at(self, t: Union[float, "dt.datetime"]) -> int: ...

    @abstractmethod
    def get_redis(self): ...

    @property
    @abstractmethod
    def reserved(self) -> int:
        """Capacity reserved"""
        ...


class SyncRepositoryService(BaseRepositoryService[SyncSchwabProducer]):

    def get_available_executed_capacity(self):
        capacity_executed = self.get_capacity_executed()
        if TYPE_CHECKING:
            assert isinstance(capacity_executed, int)
        return self.rolling_capacity - capacity_executed

    def get_available_scheduling_capacity(self):
        capacity_scheduled = self.get_capacity_scheduled()
        if TYPE_CHECKING:
            assert isinstance(capacity_scheduled, int)
        return self.scheduling_capacity - capacity_scheduled

    def get_all_request_times(self, type: Type[RequestTimesT]) -> RequestTimesT:
        executed = self.get_executed_request_times()
        scheduled = self.get_scheduled_request_times()

        if TYPE_CHECKING:
            assert isinstance(executed, list)
            assert isinstance(scheduled, list)

        if type == Tuple[List[float], List[float]]:
            ret = (executed, scheduled)
        else:
            ret = executed + scheduled

        return cast(RequestTimesT, ret)

    def get_all_next_times(self) -> List[float]:
        """Get list of already executed and scheduled request times + 60 seconds"""
        return self.get_all_request_times(List[float])

    def has_available_executed_capacity(self) -> bool:
        return self.get_available_executed_capacity() > 0

    def has_available_scheduling_capacity(self) -> bool:
        return self.get_available_scheduling_capacity() > 0

    def get_wait_time(self) -> float:
        """Returns the number of seconds until client will have capacity"""
        return max(self._get_next_capacity_timestamp() - time.time(), 0.0)

    def get_next_datetime(self) -> "dt.datetime":
        """Returns the datetime of when the client will next have capacity"""
        return dt.datetime.fromtimestamp(self._get_next_capacity_timestamp())

    def _get_next_capacity_timestamp(
        self,
    ) -> float:
        """Timestamp at which executed capacity will next be available"""
        if self.has_available_executed_capacity():
            return time.time()
        else:
            all_times = self.get_all_request_times(List[float])
            traling_times = all_times[-self.rolling_capacity :]
            return traling_times[0] + self.rolling_window

    def get_available_capacities(self) -> Tuple[int, int]:
        return (
            self.get_available_executed_capacity(),
            self.get_available_scheduling_capacity(),
        )

    def full_executed_capacity_available(self) -> bool:
        return self.get_available_executed_capacity() == self.rolling_capacity

    def get_available_queue_capacity(self) -> int:
        return self.queue_capacity - self.get_capacity_queued()

    def has_available_queue_capacity(self) -> bool:
        return self.get_available_queue_capacity() > 0

    def _get_trailing_times(self) -> List[float]:
        all_req_times = self.get_all_request_times(List[float])
        trailing_times = all_req_times[-self.rolling_capacity :]
        return trailing_times

    def _get_next_scheduling_times(self, n: int) -> List[float]:
        trailing_times = (self._get_trailing_times())[:n]
        return [t + self.rolling_window for t in trailing_times]

    @property
    def queue(self) -> "SyncRedisQueue":
        return self._get_redis_helper(
            self.redis_keys.queue, SyncRedisQueue, idx=self.idx
        )

    @property
    def sched(self) -> "SyncRedisScheduled":
        return self._get_redis_helper(
            self.redis_keys.sched, SyncRedisScheduled, idx=self.idx
        )

    @property
    def executed(self) -> "SyncRedisExecuted":
        return self._get_redis_helper(
            self.redis_keys.executed, SyncRedisExecuted, idx=self.idx
        )

    @property
    def task_db(self) -> "SyncRedisRequestTasks":
        return self._get_redis_helper(
            self.redis_keys.tasks, SyncRedisRequestTasks, idx=self.idx
        )

    @property
    def reserved(self):
        with self.get_redis() as conn:
            ret = conn.get(self.redis_keys.reserved)
        if ret is None:
            return 0
        else:
            return int(ret)

    def _await_capacity(self):
        wait_time = self.get_wait_time()
        if wait_time > 0:
            self.get_logger_adapter().debug(
                f"Awaiting capacity for {wait_time} seconds"
            )
            time.sleep(wait_time)
        return self._client

    def get_capacity_at(self, t: Union[float, "dt.datetime"]) -> int:
        """Returns the projected capacity at a future time

        :param t: the future time
        :type t: Union[float, dt.datetime]
        :param future_req_times: a list of future request times to include, defaults to None
        :type future_req_times: Optional[List[Union[float, dt.datetime]]], optional
        :return: the projected capacity at time t
        :rtype: int
        """
        target_time = t.timestamp() if isinstance(t, dt.datetime) else t
        cutoff_time = target_time - self.rolling_window
        all_request_times = self.executed.sorted() + self.sched.sorted()
        projected_request_times = [
            t for t in all_request_times if cutoff_time <= t <= target_time
        ]
        return self.rolling_capacity - len(projected_request_times)

    @contextmanager
    def get_redis(self) -> Generator[redis.Redis, Any, None]:
        with self.Manager.get_sync_connection() as conn:
            yield conn


class AsyncRepositoryService(BaseRepositoryService[AsyncSchwabProducer]):

    class Available:
        def __init__(
            self,
            executed,
            rolling_capacity,
            sched,
            scheduling_capacity,
            queue,
            queue_capacity,
        ):
            self._executed = executed
            self._rolling_capacity = rolling_capacity
            self._sched = sched
            self._scheduling_capacity = scheduling_capacity
            self._queue = queue
            self._queue_capacity = queue_capacity

        @property
        async def executed(self):
            return await self._rolling_capacity - await self._executed.get_length()

        @property
        async def sched(self):
            return await self._scheduling_capacity - await self._sched.get_length()

        @property
        async def queue(self):
            return self._queue_capacity - await self._queue.qsize()

    @cached_property
    def available(self):
        return self.Available(
            self.executed,
            self.rolling_capacity,
            self.sched,
            self.scheduling_capacity,
            self.queue,
            self.queue_capacity,
        )

    @property
    async def rolling_capacity(self) -> int:
        return self._rolling_capacity - await self.reserved

    @property
    async def scheduling_capacity(self) -> int:
        return self._scheduling_capacity - await self.reserved

    async def get_available_executed_capacity(self):
        return await self.rolling_capacity - await self.get_capacity_executed()

    async def get_available_scheduling_capacity(self):
        return await self.scheduling_capacity - await self.get_capacity_scheduled()

    async def get_all_request_times(self, type: Type[RequestTimesT]) -> RequestTimesT:
        executed = await self.get_executed_request_times()
        scheduled = await self.get_scheduled_request_times()
        if type == Tuple[List[float], List[float]]:
            ret = (executed, scheduled)
        else:
            ret = executed + scheduled
        return cast(RequestTimesT, ret)

    async def get_all_next_times(self) -> List[float]:
        """Get list of already executed and scheduled request times + 60 seconds"""
        return await self.get_all_request_times(List[float])

    async def has_available_executed_capacity(self) -> bool:
        return await self.get_available_executed_capacity() > 0

    async def has_available_scheduling_capacity(self) -> bool:
        return await self.get_available_scheduling_capacity() > 0

    async def get_wait_time(self) -> float:
        """Returns the number of seconds until client will have capacity"""
        return max(await self._get_next_capacity_timestamp() - time.time(), 0.0)

    async def get_next_datetime(self) -> "dt.datetime":
        """Returns the datetime of when the client will next have capacity"""
        return dt.datetime.fromtimestamp(await self._get_next_capacity_timestamp())

    async def _get_next_capacity_timestamp(
        self,
    ) -> float:
        """Timestamp at which executed capacity will next be available"""
        if await self.has_available_executed_capacity():
            return time.time()
        else:
            all_times = await self.get_all_request_times(List[float])
            traling_times = all_times[-(await self.rolling_capacity) :]
            return traling_times[0] + self.rolling_window

    async def get_available_capacities(self) -> Tuple[int, int]:
        return (
            await self.get_available_executed_capacity(),
            await self.get_available_scheduling_capacity(),
        )

    async def full_executed_capacity_available(self) -> bool:
        return (
            await self.get_available_executed_capacity() == await self.rolling_capacity
        )

    async def get_available_queue_capacity(self) -> int:
        return self.queue_capacity - await self.get_capacity_queued()

    async def has_available_queue_capacity(self) -> bool:
        return await self.get_available_queue_capacity() > 0

    async def _get_trailing_times(self) -> List[float]:
        all_req_times = await self.get_all_request_times(List[float])
        trailing_times = all_req_times[-(await self.rolling_capacity) :]
        return trailing_times

    async def _get_next_scheduling_times(self, n: int) -> List[float]:
        trailing_times = (await self._get_trailing_times())[:n]
        return [t + self.rolling_window for t in trailing_times]

    @property
    def queue(self) -> "AsyncRedisQueue":
        return self._get_redis_helper(
            self.redis_keys.queue, AsyncRedisQueue, idx=self.idx
        )

    @property
    def sched(self) -> "AsyncRedisScheduled":
        return self._get_redis_helper(
            self.redis_keys.sched, AsyncRedisScheduled, idx=self.idx
        )

    @property
    def executed(self) -> "AsyncRedisExecuted":
        return self._get_redis_helper(
            self.redis_keys.executed, AsyncRedisExecuted, idx=self.idx
        )

    @property
    def task_db(self) -> "AsyncRedisRequestTasks":
        return self._get_redis_helper(
            self.redis_keys.tasks, AsyncRedisRequestTasks, idx=self.idx
        )

    @property
    async def reserved(self):
        async with self.get_redis() as conn:
            ret = await conn.get(self.redis_keys.reserved)
        if ret is None:
            return 0
        else:
            return int(ret)

    async def _await_capacity(self) -> AsyncSchwabProducer:
        wait_time = await self.get_wait_time()
        if wait_time > 0:
            self.get_logger_adapter().debug(
                f"Awaiting capacity for {wait_time} seconds"
            )
            await asyncio.sleep(wait_time)
        return self._client

    async def get_capacity_at(self, t: Union[float, "dt.datetime"]) -> int:
        """Returns the projected capacity at a future time

        :param t: the future time
        :type t: Union[float, dt.datetime]
        :param future_req_times: a list of future request times to include, defaults to None
        :type future_req_times: Optional[List[Union[float, dt.datetime]]], optional
        :return: the projected capacity at time t
        :rtype: int
        """
        target_time = t.timestamp() if isinstance(t, dt.datetime) else t
        cutoff_time = target_time - self.rolling_window
        all_request_times = await self.executed.sorted() + await self.sched.sorted()
        projected_request_times = [
            t for t in all_request_times if cutoff_time <= t <= target_time
        ]
        return await self.rolling_capacity - len(projected_request_times)

    @asynccontextmanager
    async def get_redis(self) -> AsyncGenerator[aioredis.Redis, Any]:
        async with self.Manager.get_async_connection() as conn:
            yield conn


@overload
def get_repository_service(
    client: AsyncSchwabProducer,
    rolling_capacity: int | None = None,
    scheduling_capacity: int | None = None,
    queue_capacity: int | None = None,
) -> AsyncRepositoryService: ...


@overload
def get_repository_service(
    client: SyncSchwabProducer,
    rolling_capacity: int | None = None,
    scheduling_capacity: int | None = None,
    queue_capacity: int | None = None,
) -> SyncRepositoryService: ...


def get_repository_service(
    client: Union[AsyncSchwabProducer, SyncSchwabProducer],
    rolling_capacity: int | None = None,
    scheduling_capacity: int | None = None,
    queue_capacity: int | None = None,
) -> Union[AsyncRepositoryService, SyncRepositoryService]:
    if isinstance(client, AsyncSchwabProducer):
        return AsyncRepositoryService(
            client,
            rolling_capacity=rolling_capacity,
            scheduling_capacity=scheduling_capacity,
            queue_capacity=queue_capacity,
        )
    else:
        return SyncRepositoryService(
            client,
            rolling_capacity=rolling_capacity,
            scheduling_capacity=scheduling_capacity,
            queue_capacity=queue_capacity,
        )
