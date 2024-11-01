# aiohttp.ClientSession compliant typing for query parameters
import datetime as dt
from typing import (
    Any,
    Generic,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import aiohttp
import requests


QueryValueT = int | str | float
QueryVarT = QueryValueT | Sequence["QueryValueT"]
QueryT = Optional[
    str | MutableMapping[str, QueryVarT] | Sequence[Tuple[str, QueryVarT]]
]

_R = TypeVar("_R", requests.Response, aiohttp.ClientResponse)


class _TaskInfo(NamedTuple):
    start_dt: dt.datetime
    elapsed: Optional[dt.timedelta] = None
    error: Optional[Union[Exception, BaseException]] = None
    response: Optional[Any] = None

    @property
    def fetchTime(self) -> dt.datetime:
        if self.elapsed is not None:
            return self.start_dt + self.elapsed
        else:
            return self.start_dt
