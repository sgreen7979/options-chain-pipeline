#!/usr/bin/env python3
import asyncio
import atexit
from contextlib import asynccontextmanager, contextmanager
import datetime as dt
import itertools
import json
from logging import FileHandler
import os
import threading
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Concatenate,
    Generator,
    Generic,
    List,
    Optional,
    ParamSpec,
    Type,
    TYPE_CHECKING,
    TypeVar,
)

import msvcrt
import psutil

from daily.utils.classproperty import classproperty
from daily.utils.logging import ClassNameLoggerMixin
from daily.utils.redis import RedisConnectionManager

from ..client.functions import get_all_clients, setSchwabClientFactory

# from ..producer.mixins import AbstractSchwabProducerMixin
from ..producer.client.asynchronous import AsyncSchwabProducer
from ..producer.client.synchronous import SyncSchwabProducer
from ..requestlib.priority import Priority

if TYPE_CHECKING:
    from io import TextIOWrapper, _WrappedBuffer


# _T = TypeVar("_T", bound=AbstractSchwabProducerMixin)
_T = TypeVar("_T", SyncSchwabProducer, AsyncSchwabProducer)
_RetType = TypeVar("_RetType")
_redis_manager = RedisConnectionManager(
    async_max_connections=20000, async_min_connections_per_pool=50
)
_P = ParamSpec("_P")


class ClientPool(Generic[_T], ClassNameLoggerMixin):

    _lock: ClassVar[threading.RLock] = threading.RLock()
    _lock_file_descriptor: ClassVar["TextIOWrapper[_WrappedBuffer]"]
    _lock_file_released: ClassVar[bool]
    _pid: ClassVar[int]
    _command_line: ClassVar[str]
    _create_time: ClassVar[float]

    _client_threads: List[threading.Thread]
    closing: bool
    started: bool
    stopped: bool

    @classproperty
    @classmethod
    def LOCK_FILE(cls):
        return os.path.join(os.path.dirname(__file__), f"{cls.__name__}.lock")

    @contextmanager
    @classmethod
    def _acquire_lock(cls):
        if cls._lock is not None:
            cls._lock.acquire()
            try:
                yield
            except Exception as e:
                cls.get_logger().error(e)
            finally:
                cls._lock.release()

    @classmethod
    def _create_lock_file(cls):
        """
        Opens the lock file and acquires an exclusive lock using msvcrt to ensure that
        only one instance of the ClientGroup class is running. If the lock is successfully
        acquired, it writes process metadata (such as PID, command line, and creation time)
        to the file. The lock remains held for the duration of the process, preventing other
        processes from acquiring it. If the lock cannot be acquired, the method raises an
        error indicating that another instance is already running.
        """
        # Open the file without locking yet
        lock_file_descriptor = open(cls.LOCK_FILE, 'w')
        try:
            # Use msvcrt to lock the file on Windows, ensuring only one process can hold the lock
            msvcrt.locking(lock_file_descriptor.fileno(), msvcrt.LK_NBLCK, 300)
            # Lock the file exclusively (other processes will fail to acquire the lock) on Unix
            cls.get_logger().info(f"{cls.__name__} lock acquired")
        except OSError:
            cls.get_logger().error(
                f"{cls.__name__} is already running, cannot acquire lock"
            )
            lock_file_descriptor.close()
            raise RuntimeError(f"{cls.__name__} is already running.")
        else:
            cls._pid = os.getpid()
            process = psutil.Process(cls._pid)
            cls._command_line = " ".join(process.cmdline())
            cls._create_time = process.create_time()
            proc_data = {
                "pid": cls._pid,
                "cmdline": cls._command_line,
                "create_time": cls._create_time,
            }
            # proc_data_dumps = json.dumps(proc_data, indent=4)
            json.dump(proc_data, lock_file_descriptor)
            lock_file_descriptor.flush()  # Ensure the data is written to disk

        cls._lock_file_descriptor = lock_file_descriptor
        cls._lock_file_released = False
        atexit.register(cls._remove_lock_file)

    @classmethod
    def _remove_lock_file(cls):
        if hasattr(cls, "_lock_file_descriptor") and not cls._lock_file_released:
            try:
                # Unlock the file
                msvcrt.locking(cls._lock_file_descriptor.fileno(), msvcrt.LK_UNLCK, 300)
                cls.get_logger().info(f"{cls.__name__} lock released")
            except OSError:
                cls.get_logger().error("Failed to release ClientGroup lock")
            finally:
                cls._lock_file_descriptor.close()
                if os.path.exists(cls.LOCK_FILE):
                    os.remove(cls.LOCK_FILE)
                    cls.get_logger().info("SchwabClientPool lock file removed")
            cls._lock_file_released = True  # Set flag to prevent duplicate calls

    @classmethod
    def is_running(cls) -> bool:
        """
        Check if the ClientGroup instance is running by verifying the existence of the
        lock file and ensuring the process that created it is still active and matches
        the stored start time (to avoid PID recycling). If the process is no longer active
        or the lock file is corrupted, the lock file is removed to prevent orphaned locks.
        """
        # return os.path.exists(ClientGroup.LOCK_FILE)
        if not os.path.exists(cls.LOCK_FILE):
            return False  # No lock file, so no active instance

        try:
            # Read the lock file to get the process information
            with open(cls.LOCK_FILE, 'r') as lock_file:
                lock_info = json.load(lock_file)
                pid = lock_info.get('pid')
                start_time = lock_info.get('create_time')

                # Check if the process is still running
                if pid and psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    # Verify that the process start time matches the stored start time
                    if process.create_time() == start_time:
                        return True  # Process with the PID and start time matches, instance is running
                    else:
                        # Process exists but has a different start time (PID was recycled)
                        cls.get_logger().info(
                            "PID was recycled, removing orphaned lock file."
                        )
                        os.remove(cls.LOCK_FILE)
                        return False
                else:
                    # Orphaned lock file, process no longer exists
                    cls.get_logger().info("Orphaned lock file found, removing.")
                    os.remove(cls.LOCK_FILE)
                    return False
        except PermissionError:
            return True

        except (OSError, json.JSONDecodeError):
            # Handle cases where the lock file is corrupted or unreadable
            cls.get_logger().error("Lock file is unreadable or corrupted, removing it.")
            os.remove(cls.LOCK_FILE)
            return False

    def __init__(self, clients: Optional[List[_T]] = None) -> None:
        self.get_logger().debug("ClientPool.__init__")
        self.get_logger().debug(f"clients: {clients}")
        self._clients: List[_T] = clients or get_all_clients()

        if not self._clients:
            raise RuntimeError("No clients")

        for c in self._clients:
            try:
                c.set_pool(self)  # type: ignore
            except AttributeError:
                setattr(c, "_pool", self)

        self.idx_cycle = itertools.cycle(self.get_idxs())

    @classmethod
    def __init_subclass__(cls, client_type: Type[_T], **kwargs):
        cls.get_logger().debug("ClientPool.__init_subclass__")
        cls.get_logger().debug(f"client_type: {client_type}")
        cls.get_logger().debug(f"kwargs: {kwargs}")
        super().__init_subclass__(**kwargs)
        from ..client.base import BaseSchwabClient

        # issubclass(Foo, Foo) returns True, so we don't have to also
        # check if client_type is BaseSchwabClient
        assert issubclass(client_type, BaseSchwabClient)
        setSchwabClientFactory(client_type)

    def get_idxs(self):
        return sorted([c.idx for c in self._clients])

    def get_log_filename(self) -> Optional[str]:
        if filehandlers := [
            h for h in self.get_logger().handlers if isinstance(h, FileHandler)
        ]:
            return filehandlers[0].baseFilename.split("\\")[-1]

    def get_client_by_idx(self, idx: int):
        return list(filter(lambda c: c.idx == idx, self._clients))[0]

    def next_client(self):
        idx = next(self.idx_cycle)
        client = self.get_client_by_idx(idx)
        self.get_logger().debug(f"Acquired client {idx}")
        return client

    def __len__(self) -> int:
        return len(self._clients)

    def __iter__(self):
        yield from self._clients

    def index(self, client: _T) -> int:
        return self._clients.index(client)

    def __getitem__(self, idx: int) -> _T:
        return self._clients[idx]

    @asynccontextmanager
    @staticmethod
    async def get_redis_async():
        # return aioredis.from_url("redis://127.0.0.1:6379/0", max_connections=1024)
        async with _redis_manager.get_async_connection() as conn:
            yield conn

    @contextmanager
    @staticmethod
    def get_redis_sync():
        # return aioredis.from_url("redis://127.0.0.1:6379/0", max_connections=1024)
        with _redis_manager.get_sync_connection() as conn:
            yield conn

    def _await_client_with_capacity(self, *args, **kwargs) -> _T:
        raise NotImplementedError

    @property
    def client_threads(self) -> List[threading.Thread]:
        return self._client_threads

    def clear_queues(self):
        raise NotImplementedError

    def log_updates(self):
        raise NotImplementedError

    def _get_wait_times(self) -> List[float]:
        raise NotImplementedError

    def _get_next_dts(self) -> List[dt.datetime]:
        raise NotImplementedError

    def _get_next_ts(self) -> List[float]:
        raise NotImplementedError

    def yield_client(
        # self, incl_queued: bool = False, freeze: bool = False
        self,
        urgent: bool = True,
        # ) -> AsyncGenerator["SchwabClient", Any, None]:
    ) -> Generator:
        raise NotImplementedError

    def no_current_capacity(self) -> bool:
        raise NotImplementedError

    def get_capacity_used(self) -> int:
        raise NotImplementedError

    def get_capacity_queued(self) -> int:
        raise NotImplementedError

    def get_capacity_scheduled(self) -> int:
        raise NotImplementedError

    def get_current_capacity(self) -> int:
        raise NotImplementedError

    def has_capacity(self) -> bool:
        raise NotImplementedError

    def get_capacity_summary(self, incl_queued: bool = True):
        raise NotImplementedError

    def get_request_log(self):
        raise NotImplementedError

    @property
    def rolling_capacity(self) -> int:
        # return sum(c.rolling_capacity for c in self._clients)
        raise NotImplementedError

    def start(
        self,
        callback: Optional[Callable] = None,
        endpoint: Optional[str] = None,
        join: bool = False,
    ) -> List[threading.Thread]:
        raise NotImplementedError

    def stop(self, hard_stop: bool = False):
        if self.started and not self.stopped:
            self.closing = True

            for client in self._clients:
                client.stop(hard_stop=hard_stop)

            if self.client_threads:
                for client_thread in self.client_threads:
                    try:
                        client_thread.join()
                    except Exception as e:
                        self.get_logger().warning(
                            f"Failed to join client thread {str(e)}"
                        )

            self.stopped = True
            self.__class__._remove_lock_file()

    def map(
        self,
        fn: Callable[Concatenate[_T, _P], _RetType],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> List[_RetType]:
        """
        Map a given function that must accept the producer client
        as the first argument, may accept some other arguments,
        and returns some type, to all clients in the pool.
        """
        return list(map(lambda client: fn(client, *args, **kwargs), self._clients))

    async def amap(
        self,
        fn: Callable[Concatenate[_T, _P], Awaitable[_RetType]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> List[_RetType]:
        """
        Map a given async function that must accept the producer
        client as the first argument, may accept some other
        arguments, and returns some type, to all clients in the
        pool.
        """
        tasks = [fn(c, *args, **kwargs) for c in self._clients]
        return await asyncio.gather(*tasks)

    Priority = Priority
