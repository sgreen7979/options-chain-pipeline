#!/usr/bin/env python3
import asyncio
import contextvars
import datetime as dt
import functools
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Generic,
    Hashable,
    Literal,
    Optional,
    Self,
    Set,
    Type,
    TypeGuard,
    TypeVar,
    Union,
)
import weakref

import aiohttp
from pydantic import BaseModel, Field, field_serializer

from daily.utils.logging import get_logger, ClassNameLoggerMixin

from .make import MakeRequestModel
from .priority import Priority
from ..api import Endpoint
from .instrument import Instrument
from .summary import _SummaryModel
from .types import _R

if TYPE_CHECKING:
    from ..producer.client.synchronous import SyncSchwabProducer
    from ..producer.client.asynchronous import AsyncSchwabProducer


LOG_LEVEL = "INFO"
logger = get_logger(__name__, LOG_LEVEL, ch=True)

_T = TypeVar("_T", bound="BaseRequestTaskModel")


model_registry: weakref.WeakValueDictionary[Hashable, "BaseRequestTaskModel"] = (
    weakref.WeakValueDictionary()
)


def get_task_by_uuid(task_type: Type[_T], uuid: str) -> Optional[_T]:
    task = model_registry.get(uuid)
    if isinstance(task, task_type):
        return task
    return None


class BaseRequestTaskModel(BaseModel, ClassNameLoggerMixin, Generic[_R]):
    """
    This class represents the base request task model supporting
    Schwab api requests executed in this module. This class is
    designed to facilitate execution, monitoring, logging, and
    exception handling of Schwab requests. request tasks in highly concurrent, long-running processes
    during market hours
    """

    _instruments: ClassVar[Set[Instrument]] = set()

    @classmethod
    def attach_instrument(cls, instrument: Instrument):
        cls._instruments.add(instrument)

    def __init__(self, **kwargs) -> None:
        if kwargs.get("summary_json", None) is None:
            make_request = kwargs["make_request"]
            summary_json = _SummaryModel[_R](
                uuid=make_request.uuid,
                capture_output=make_request.capture_output,
                params=make_request.params,
                endpoint=make_request.endpoint,
            )
            kwargs["summary_json"] = summary_json
        super().__init__(**kwargs)
        self._cancelled: bool = False
        self.summary_json.time_created = self.time_created
        model_registry[self.uuid] = self
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_create(self)
        # weakref.finalize(self, lambda self: del self)

    # def model_post_init(self, __context: Any = None):
    #     self.summary_json: _SummaryModel[_R] = self.get_summary_json()

    class Config:
        arbitrary_types_allowed = True

    make_request: MakeRequestModel
    client_idx: Optional[int] = Field(default=None)
    time_created: str = Field(default_factory=lambda: dt.datetime.now().isoformat())
    time_staged: Optional[str] = Field(default=None)
    time_scheduled: Optional[str] = Field(default=None)
    time_executed: Optional[str] = Field(default=None)
    summary_json: _SummaryModel[_R]
    context_set_time: Optional[str] = Field(default=None)

    @property
    def uuid(self) -> str:
        return self.make_request.uuid

    @staticmethod
    def _decode_time(t: Union[float, dt.datetime, str]) -> str:
        if isinstance(t, str):
            return t
        elif isinstance(t, dt.datetime):
            return t.isoformat()
        elif isinstance(t, float):
            return dt.datetime.fromtimestamp(t).isoformat()  # noqa: F841
        else:
            raise ValueError(
                f"Expected string, float, or dt.datetime, got {type(t).__name__}"
            )

    def set_time_staged(self, t: Union[float, dt.datetime, str]):
        self.time_staged = self._decode_time(t)

    def set_time_scheduled(self, t: Union[float, dt.datetime, str]):
        self.time_scheduled = self._decode_time(t)
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_scheduled(self)

    def set_time_executed(self, t: Union[float, dt.datetime, str]):
        self.time_executed = self._decode_time(t)
        if self.summary_json.fetchTime is None:
            self.summary_json.fetchTime = self.time_executed
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_executed(self)

    def set_context_set_time(self, t: Union[float, dt.datetime, str]):
        self.context_set_time = self._decode_time(t)

    def get_time_staged_dt(self):
        if self.time_staged is not None:
            return dt.datetime.fromisoformat(self.time_staged)

    def get_time_scheduled_dt(self):
        if self.time_scheduled is not None:
            return dt.datetime.fromisoformat(self.time_scheduled)

    def set_error(self, err: Union[Exception, BaseException]):
        self.summary_json.set_error(err)
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_set_error(self)

    # @computed_field
    # @property
    # def time_executed(self) -> Optional[str]:
    #     return self.summary_json.fetchTime

    @property
    def params(self) -> Optional[dict]:
        return self.make_request.params

    def set_auth_request_headers(self, headers: dict):
        self.make_request.headers.update(headers)

    def get_context(
        self,
        task_var: contextvars.ContextVar["BaseRequestTaskModel"],
    ) -> contextvars.Context:
        """
        Set the given task var and return the current context
        """
        token = task_var.set(self)
        self.set_context_set_time(dt.datetime.now())
        self._token = token
        return contextvars.copy_context()

    # @computed_field
    @property
    def last_task(self) -> Optional["BaseRequestTaskModel[_R]"]:
        if hasattr(self, "_token"):
            return self._token.old_value

    @classmethod
    def get_by_uuid(cls, uuid: str):
        """
        Retrieve an instance from the model registry using the key.
        Returns None if no instance is found for the given key.
        """
        return get_task_by_uuid(cls, uuid)

    @property
    def capture_output(self) -> bool:
        """
        Persist the fetched response data to redis using the
        client's redis log key that was assigned to this task

        redis key:`client_from_idx(self.client_idx).redis_keys.log`
        """
        return self.make_request.capture_output

    def done(self) -> bool:
        """
        Returns True if the attr:`summary_model.response`
        or attr:`summary_model.error` is set
        """
        return self.summary_json is not None and (
            self.summary_json.error is not None
            or self.summary_json.response is not None
        )

    def ok(self) -> bool:
        """
        Assert that the task is complete and that the status
        code of the resulting response equals 200
        """
        try:
            assert self.done()
            assert self.summary_json is not None
            assert self.summary_json.response is not None
            if isinstance(self.summary_json.response, aiohttp.ClientResponse):
                return self.summary_json.response.status == 200
            else:
                return self.summary_json.response.status_code == 200
        except AssertionError:
            return False

    def assign(self, client_idx: int) -> Self:
        if self.done():
            type(self).get_logger.error(
                f"Cannot assign client to a completed task {self.uuid}"
            )
        self.client_idx = client_idx
        self.summary_json.client_idx = client_idx
        return self

    # def get_summary_json(self) -> _SummaryModel[_R]:
    #     return _SummaryModel[_R](
    #         uuid=self.uuid,
    #         capture_output=self.capture_output,
    #         client_idx=self.client_idx,
    #         params=self.make_request.params,
    #         time_created=self.time_created,
    #         endpoint=self.make_request.endpoint,
    #         time_staged=self.time_staged,
    #     )

    @property
    def url(self) -> str:
        if isinstance(self.make_request.endpoint, Endpoint):
            return self.make_request.endpoint.url
        else:
            return f"https://api.schwabapi.com/{self.make_request.endpoint}"

    def complete(self):
        if not self.done():
            self.get_logger().error("Cannot call complete on an incomplete task")
        elif type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_completion(self)

    def cancel(self):
        if not self.done() and not self._cancelled:
            self._cancelled = True
            self.summary_json.set_error(
                asyncio.CancelledError(f"task {self.uuid} cancelled")
            )
            if type(self)._instruments:
                for instrument in type(self)._instruments:
                    instrument.on_task_cancellation(self)


class SyncRequestTaskModel(BaseRequestTaskModel[_R]):

    @staticmethod
    def _is_sync_producer(client) -> TypeGuard["SyncSchwabProducer"]:
        from ..producer.client.synchronous import SyncSchwabProducer

        return isinstance(client, SyncSchwabProducer)

    def stage(self, mode: Literal["uuid", "make_request"]) -> Self:
        if self.client_idx is None:
            raise RuntimeError("client was never set")

        from ..client.functions import client_from_idx

        client = client_from_idx(self.client_idx)
        assert self._is_sync_producer(client), type(client).__name__

        item = self.uuid if mode == "uuid" else self.make_request.model_dump_json()

        if self.make_request.priority == Priority.URGENT:
            client.event.set()
            client.queue.insertleft(item)
            client.event.clear()
        else:
            client.queue.insertright(item)

        self.time_staged = dt.datetime.now().isoformat()
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_stageed(self)
        return self

    def save(self) -> Self:
        save_sync_request_task(self)
        return self

    def assert_saved(self) -> Self:
        _assert_saved_sync(self)
        return self


class AsyncRequestTaskModel(BaseRequestTaskModel[_R]):
    future: Optional["asyncio.Future"] = Field(exclude=True, default=None)
    target_timestamp: Optional[float] = Field(default=None)
    target_loop_time: Optional[float] = Field(default=None)
    wrapped_callback: Optional[Callable] = Field(default=None)
    timer_handle: Optional["asyncio.TimerHandle"] = Field(exclude=True, default=None)
    loop: Optional["asyncio.AbstractEventLoop"] = Field(exclude=True, default=None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc, exc_type, tb):
        pass

    @field_serializer('wrapped_callback', when_used="unless-none")
    def serialize_wrapped_callback(self, wrapped_callback: Callable):
        return wrapped_callback.__qualname__

    @staticmethod
    def _is_async_producer(client) -> TypeGuard["AsyncSchwabProducer"]:
        from ..producer.client.asynchronous import AsyncSchwabProducer

        return isinstance(client, AsyncSchwabProducer)

    def get_context(
        self,
        task_var: contextvars.ContextVar["AsyncRequestTaskModel"],
    ) -> contextvars.Context:
        """
        Set the given task var and return the current context
        """
        token = task_var.set(self)
        self.set_context_set_time(dt.datetime.now())
        self._token = token
        return contextvars.copy_context()

    def set_loop(self, loop: "asyncio.AbstractEventLoop"):
        self.loop = loop
        self.future = self.loop.create_future()
        return self

    async def set_future(self):
        self.future = asyncio.get_running_loop().create_future()
        return self.future

    async def stage(self, mode: Literal["uuid", "make_request"]):
        if self.client_idx is None:
            raise RuntimeError("client was never set")

        from ..client.functions import client_from_idx
        from ..producer.client.asynchronous import AsyncSchwabProducer

        client = client_from_idx(self.client_idx, client_factory=AsyncSchwabProducer)

        item = self.uuid if mode == "uuid" else self.make_request.model_dump_json()

        if self.make_request.priority == Priority.URGENT:
            client._pause_event.set()
            await client.queue.insertleft(item)
            client._pause_event.clear()
        else:
            await client.queue.insertright(item)

        self.time_staged = dt.datetime.now().isoformat()
        self.summary_json.time_staged = self.time_staged
        if type(self)._instruments:
            for instrument in type(self)._instruments:
                instrument.on_task_stageed(self)
        return self

    def get_wrapped_callback(
        self,
        # loop: "asyncio.AbstractEventLoop",
        callback,
        done_callback: Optional[Callable] = None,
    ):
        assert self.future is not None
        self.wrapped_callback = self.wrap_callback_in_future(
            callback, self.future, done_callback
        )
        return self.wrapped_callback

    def wrap_callback_in_future(
        self,
        callback: Callable,
        future: "asyncio.Future",
        done_callback: Optional[Callable] = None,
    ):
        @functools.wraps(callback)
        def wrapped_callback(*args, **kwargs):
            if done_callback is not None:
                future.add_done_callback(done_callback)
            try:
                result = callback(*args, **kwargs)
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                # In case of an error, set the exception in the future
                result = e
                if not future.done():
                    future.set_exception(e)
            finally:
                # self.time_executed = dt.datetime.now().isoformat()
                return result

        return wrapped_callback

    async def save(self):
        await save_async_request_task(self)
        return self

    async def assert_saved(self):
        await _assert_saved_async(self)
        return self


############################################################
# Synchronous helper functions for saving and retrieving
# synchronous request tasks


def _assert_saved_sync(task: "SyncRequestTaskModel"):
    assert task.client_idx is not None
    return get_sync_request_task_by_uuid(task.uuid, task.client_idx) is not None


def save_sync_request_task(task: "SyncRequestTaskModel"):
    assert task.client_idx is not None
    from ..client.functions import client_from_idx

    client = client_from_idx(task.client_idx)
    client.task_db.add_task(task)


def get_sync_request_task_by_uuid(
    uuid: str,
    client_idx: int,
) -> Optional[dict]:
    """Retrieve the task by its UUID"""
    if task := SyncRequestTaskModel.get_by_uuid(uuid):
        return task.make_request.model_dump()
    else:
        from ..client.functions import client_from_idx

        client = client_from_idx(client_idx)
        return client.task_db.get_task(uuid)


############################################################
# Asynchronous helper functions for saving and retrieving
# asynchronous request tasks


async def _assert_saved_async(task: "AsyncRequestTaskModel"):
    assert task.client_idx is not None
    return get_async_request_task_by_uuid(task.uuid, task.client_idx) is not None


async def save_async_request_task(task: "AsyncRequestTaskModel"):
    assert task.client_idx is not None
    from ..client.functions import client_from_idx

    client = client_from_idx(task.client_idx)
    await client.task_db.add_task(task)


async def get_async_request_task_by_uuid(
    uuid: str,
    client_idx: int,
) -> Optional[dict]:
    """Retrieve the task by its UUID"""
    if task := AsyncRequestTaskModel.get_by_uuid(uuid):
        return task.make_request.model_dump()
    else:
        from ..client.functions import client_from_idx

        client = client_from_idx(client_idx)
        return await client.task_db.get_task(uuid)
