import contextvars
import importlib  # unused for now
import os
from typing import TYPE_CHECKING, List, Type, TypeVar, cast

import aiohttp
import attr
import requests

from .types import _TaskInfo


if TYPE_CHECKING:
    from .task import AsyncRequestTaskModel, SyncRequestTaskModel

__all__ = ["TASK_VAR", "REQUEST_TASK_PROTOCOL_VARNAME"]

_T_co = TypeVar("_T_co", covariant=True)  # request task type
_R = TypeVar('_R')  # _R is the response type to be passed to _T


REQUEST_TASK_PROTOCOL_VARNAME = "@DAILY_REQUEST_TASK_PROTOCOL"
REQUEST_TASK_PROTOCOL = os.getenv(REQUEST_TASK_PROTOCOL_VARNAME, "ASYNC").upper()
"""Determine our request task protocol dynamically at runtime
depending on the value set to `'@DAILY_REQUEST_TASK_PROTOCOL'`.

The value of this attribute will correspond to the typed request task
context variable imported at runtime.

If a value has not been set, we default to the async protcol.
"""

TASK_INFO_VAR = contextvars.ContextVar[_TaskInfo]("TASK_INFO_VAR")


@attr.define
class MISSING:
    """Represents an unset value for the TASK_VAR"""


def __getattr__(name):
    """Catch imports of `TASK_VAR` in scenarios where it was never set.

    The current implementation herein **should** ensure such a scenario
    never occurs, but, if it does occur, we return the `MISSING`
    singleton to signal to the user that it has not yet been set.
    """
    if name == "TASK_VAR":
        return globals().get(name, MISSING)
    if name in globals():
        return globals()[name]
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )  # explicitly raise error


# TODO need to implement this
def set_task_var(var: contextvars.ContextVar):
    global TASK_VAR
    TASK_VAR = var


# TODO need to implement this
def get_task_var() -> contextvars.ContextVar:
    if globals().get("TASK_VAR", None) is None:
        raise RuntimeError("TASK_VAR is not set")
    return globals()["TASK_VAR"]


def set_contextvar(ctxvarfactory):
    global TASK_VAR
    TASK_VAR = ctxvarfactory("TASK_VAR")


def set_and_get_contextvar(ctxvarfactory):
    set_contextvar(ctxvarfactory)
    global TASK_VAR
    return TASK_VAR


def get_request_task_var_factory(
    request_task_factory: _T_co,
) -> Type[contextvars.ContextVar[_T_co]]:
    return cast(
        Type[contextvars.ContextVar[_T_co]],
        contextvars.ContextVar[request_task_factory],
    )


def get_async_request_task_factory() -> (
    Type["AsyncRequestTaskModel[aiohttp.ClientResponse]"]
):
    from .task import AsyncRequestTaskModel  # noqa: F811

    return AsyncRequestTaskModel[aiohttp.ClientResponse]


def get_sync_request_task_factory() -> Type["SyncRequestTaskModel[requests.Response]"]:
    from .task import SyncRequestTaskModel  # noqa: F811

    return SyncRequestTaskModel[requests.Response]


def get_async_request_task_var_factory():
    from .task import AsyncRequestTaskModel  # noqa: F811

    return cast(
        Type[contextvars.ContextVar[AsyncRequestTaskModel[aiohttp.ClientResponse]]],
        get_request_task_var_factory(AsyncRequestTaskModel[aiohttp.ClientResponse]),
    )


def get_and_set_async_request_task_var() -> (
    contextvars.ContextVar["AsyncRequestTaskModel[aiohttp.ClientResponse]"]
):
    TASK_VAR = globals().get("TASK_VAR", None)
    if TASK_VAR is None:
        task_var_factory = get_async_request_task_var_factory()
        TASK_VAR = task_var_factory("TASK_VAR")
    return TASK_VAR


def get_sync_request_task_var_factory() -> (
    Type[contextvars.ContextVar["SyncRequestTaskModel[requests.Response]"]]
):
    from .task import SyncRequestTaskModel  # noqa: F811

    return cast(
        Type[contextvars.ContextVar["SyncRequestTaskModel[requests.Response]"]],
        get_request_task_var_factory(SyncRequestTaskModel[requests.Response]),
    )


def get_and_set_sync_request_task_var() -> (
    contextvars.ContextVar["SyncRequestTaskModel[requests.Response]"]
):
    task_var_factory = get_sync_request_task_var_factory()
    TASK_VAR = task_var_factory("TASK_VAR")
    return TASK_VAR


_SYNC_TASK_VAR_FACTORY = get_sync_request_task_var_factory()
_ASYNC_TASK_VAR_FACTORY = get_async_request_task_var_factory()

if "async" in REQUEST_TASK_PROTOCOL.lower():
    TASK_VAR = get_and_set_async_request_task_var()
    # TASK_VAR: (  # type: ignore
    #     "contextvars.ContextVar[AsyncRequestTaskModel[aiohttp.ClientResponse]]"
    # ) = get_and_set_async_request_task_var()  # type: ignore
    """
    Our runtime request task context variable.  This is meant to
    be of particular use in asynchronous request task execution.
    """
    BATCHED_TASK_VAR: (  # type: ignore
        "contextvars.ContextVar[List[AsyncRequestTaskModel[aiohttp.ClientResponse]]]"
    ) = contextvars.ContextVar[
        List["AsyncRequestTaskModel[aiohttp.ClientResponse]"]
    ](  # type: ignore
        "BATCHED_TASK_VAR"
    )
    ACTIVE_TASK_VAR_FACTORY = _ASYNC_TASK_VAR_FACTORY
else:
    # TASK_VAR: "contextvars.ContextVar[SyncRequestTaskModel[requests.Response]]" = (
    #     get_and_set_sync_request_task_var()
    # )
    TASK_VAR = get_and_set_sync_request_task_var()
    BATCHED_TASK_VAR: (
        "contextvars.ContextVar[List[SyncRequestTaskModel[requests.Response]]]"
    ) = contextvars.ContextVar[List["SyncRequestTaskModel[requests.Response]"]](
        "BATCHED_TASK_VAR"
    )
    ACTIVE_TASK_VAR_FACTORY = _SYNC_TASK_VAR_FACTORY
    """
    Our runtime request task context variable.  This is meant to
    be of particular use in asynchronous request task execution.
    """
