import datetime as dt
import json
from typing import Dict, Generic, Optional, TypeVar, Union

import aiohttp
from pydantic import (
    BaseModel,
    computed_field,
    Field,
    field_serializer,
)
import requests

from ..api import Endpoint

# _R = TypeVar("_R", bound="ResponseProtocol")
_R = TypeVar("_R", requests.Response, aiohttp.ClientResponse)


class _SummaryModel(BaseModel, Generic[_R]):
    uuid: Optional[str] = Field(default=None)
    time_created: Optional[str] = Field(default=None)
    prepared_request: requests.PreparedRequest = Field(default=None)
    params: Optional[Dict] = Field(default=None)
    time_staged: Optional[str] = Field(default=None)
    endpoint: Union[str, Endpoint]
    client_idx: Optional[int] = Field(default=None)
    error: Optional[Union[str, Exception, BaseException]] = Field(default=None)
    # response: Optional[Union[requests.Response, aiohttp.ClientResponse]] = Field(
    # response: Optional[_R] = Field(default=None, init=False)
    # response: Optional[Union[aiohttp.ClientResponse, requests.Response]] = Field(
    response: Optional[_R] = Field(default=None, init=False)  # Allow both types
    fetchTime: Optional[str] = Field(default=None)
    order_id: Optional[str] = Field(default=None)
    capture_output: bool = Field(default=False)
    response_data: Optional[Dict] = Field(default=None)
    elapsed: Optional[dt.timedelta] = Field(default=None)
    start_dt: Optional[dt.datetime] = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("error", when_used="json-unless-none")
    def serialize_error(self, error: Union[str, Exception, BaseException]):
        return str(error) if isinstance(error, (Exception, BaseException)) else error

    @field_serializer("response_data", when_used="json-unless-none")
    def serialize_response_data(self, response_data):
        return response_data if self.capture_output else '<TOO-LARGE>'

    @field_serializer("response", when_used="json-unless-none")
    def serialize_response(self, response: _R):
        return {
            "ok": response.ok,
            "headers": dict(list(response.headers.items())),
            "status_code": (
                response.status_code
                if isinstance(response, requests.Response)
                else response.status
            ),
            "reason": response.reason,
            "text": (
                json.dumps(self.response_data)
                if self.response_data is not None
                and ((not response.ok) or (self.capture_output))
                else "<TOO-LARGE>"
            ),
            "response_class": "{}.{}".format(
                response.__class__.__module__,
                response.__class__.__name__,
            ),
        }

    def set_prepared_request(self, prepared_request: requests.PreparedRequest):
        self.prepared_request = prepared_request

    @field_serializer("prepared_request", when_used="json")
    def serialize_prepared_request(self, prepared_request: requests.PreparedRequest):
        return {
            "method": prepared_request.method,
            "body": prepared_request.body,
        }

    def set_elapsed(self, elapsed: Optional[Union[float, dt.timedelta]]):
        if isinstance(elapsed, (float, int)):
            elapsed = dt.timedelta(seconds=elapsed)
        self.elapsed = elapsed

    @field_serializer("start_dt", when_used="json-unless-none")
    def serialize_start_dt(self, start_dt: dt.datetime):
        return start_dt.isoformat()

    @field_serializer("elapsed", when_used="json-unless-none")
    def serialize_elapsed(self, elapsed: dt.timedelta):
        return elapsed.total_seconds()

    def set_start_dt(self, start_dt: dt.datetime):
        self.start_dt = start_dt

    def set_error(self, err: Union[str, Exception, BaseException]):
        self.error = err

    def set_response_data(self, data: Dict):
        self.response_data = data

    # def set_response(self, response: Union[requests.Response, aiohttp.ClientResponse]):
    def set_response(self, response: _R):
        if isinstance(response, requests.Response):
            self.response = response
            self.set_response_data(self.response.json())
            self.set_elapsed(self.response.elapsed)
        else:
            return self._set_response_async(response)

    # async def _set_response_async(self, response: aiohttp.ClientResponse):
    async def _set_response_async(self, response: _R):
        self.response = response
        self.set_response_data(await self.response.json())  # type: ignore

    # def get_resp_data(self, incl_fetch_time: bool = False, incl_response: bool = False):
    #     # if self.response is not None and self.response.ok:
    #     assert isinstance(self.response, requests.Response), self.response
    #     try:
    #         data = self.response.json()  # type: ignore
    #         if incl_fetch_time:
    #             data["fetchTime"] = self.fetchTime
    #         if incl_response:
    #             data["response"] = self.response
    #         return data
    #     except Exception:
    #         pass

    # async def get_resp_data_async(
    #     self, incl_fetch_time: bool = False, incl_response: bool = False
    # ):
    #     # if self.response is not None and self.response.ok:
    #     assert isinstance(self.response, aiohttp.ClientResponse), self.response

    #     try:
    #         data = await self.response.json()  # type: ignore
    #         if incl_fetch_time:
    #             data["fetchTime"] = self.fetchTime
    #         if incl_response:
    #             data["response"] = self.response
    #         return data
    #     except Exception:
    #         pass

    @computed_field
    @property
    def is_async(self) -> bool:
        if self.response is not None:
            return isinstance(self.response, aiohttp.ClientResponse)
        else:
            from ..client.asynchronous import AsyncSchwabClient
            from ..client.functions import getSchwabClientFactory

            return isinstance(getSchwabClientFactory(), AsyncSchwabClient)


# class SyncResponseProtocol(Protocol):
#     reason: str
#     status_code: int
#     headers: object

#     def json(self, *args, **kwargs) -> dict: ...
#     def text(self, *args, **kwargs) -> str: ...


# class AsyncResponseProtocol(Protocol):
#     reason: str
#     status: int
#     headers: object

#     async def json(self, *args, **kwargs) -> dict: ...
#     async def text(self, *args, **kwargs) -> str: ...


# ResponseProtocol = SyncResponseProtocol | AsyncResponseProtocol


# class SummaryModel(BaseModel, Generic[_R]):
#     uuid: Optional[str] = Field(default=None)
#     time_created: Optional[str] = Field(default=None)
#     prepared_request: requests.PreparedRequest
#     params: Optional[Dict] = Field(default=None)
#     time_staged: Optional[str] = Field(default=None)
#     endpoint: str
#     client_idx: Optional[int] = Field(default=None)
#     error: Optional[Union[str, Exception, BaseException]] = Field(
#         default=None, init=False
#     )
#     # response: Optional[Union[requests.Response, aiohttp.ClientResponse]] = Field(
#     response: Optional[_R] = Field(default=None, init=False)
#     fetchTime: Optional[str] = Field(default=None, init=False)
#     order_id: Optional[str] = Field(default=None, init=False)
#     capture_output: bool = Field(default=False)
#     response_data: Optional[Dict] = Field(default=None)

#     class Config:
#         arbitrary_types_allowed = True

#     @field_serializer("error", when_used="json-unless-none")
#     def serialize_error(self, error: Union[str, Exception, BaseException]):
#         return str(error) if isinstance(error, (Exception, BaseException)) else error

#     @field_serializer("response", when_used="json-unless-none")
#     def serialize_response(
#         self, response: Union[requests.Response, aiohttp.ClientResponse]
#     ):
#         return {
#             "ok": response.ok,
#             "headers": dict(list(response.headers.items())),
#             "status_code": (
#                 response.status_code
#                 if isinstance(response, requests.Response)
#                 else response.status
#             ),
#             "reason": response.reason,
#             "text": (
#                 json.dumps(self.response_data)
#                 # getattr(response, "_text")
#                 if (not response.ok) or (self.capture_output is True)
#                 else "<TOO-LARGE>"
#             ),
#             "response_class": response.__class__.__name__,
#         }

#         # if isinstance(response, requests.Response):
#         #     serialized_response.update(
#         #         {
#         #             "status_code": response.status_code,
#         #             # "headers": response.headers._store,  # type: ignore
#         #             # "elapsed": response.elapsed.total_seconds(),
#         #             # "text": (
#         #             #     response.text
#         #             #     if (not response.ok) or (self.capture_output is True)
#         #             #     else "<TOO-LARGE>"
#         #             # ),
#         #         }
#         #     )
#         # else:
#         #     serialized_response.update(
#         #         {
#         #             "status_code": response.status,
#         #             # "headers": dict(list(response.headers.items())),
#         #             # "text": (
#         #             #     json.dumps(self.response_data)
#         #             #     # getattr(response, "_text")
#         #             #     if (not response.ok) or (self.capture_output is True)
#         #             #     else "<TOO-LARGE>"
#         #             # ),
#         #         }
#         #     )
#         # return serialized_response

#     @field_serializer("prepared_request", when_used="json")
#     def serialize_prepared_request(self, prepared_request: requests.PreparedRequest):
#         return {
#             "method": prepared_request.method,
#             "body": prepared_request.body,
#         }

#     def set_error(self, err):
#         self.error = err

#     def set_response_data(self, data: Dict):
#         self.response_data = data

#     # def set_response(self, response: Union[requests.Response, aiohttp.ClientResponse]):
#     def set_response(self, response: _R):
#         if isinstance(response, requests.Response):
#             self.response = response
#             self.set_response_data(self.response.json())  # type: ignore
#         else:
#             return self._set_response_async(response)

#     # async def _set_response_async(self, response: aiohttp.ClientResponse):
#     async def _set_response_async(self, response: _R):
#         self.response = response
#         self.set_response_data(await self.response.json())  # type: ignore

#     # def get_resp_data(self, incl_fetch_time: bool = False, incl_response: bool = False):
#     #     # if self.response is not None and self.response.ok:
#     #     assert isinstance(self.response, requests.Response), self.response
#     #     try:
#     #         data = self.response.json()  # type: ignore
#     #         if incl_fetch_time:
#     #             data["fetchTime"] = self.fetchTime
#     #         if incl_response:
#     #             data["response"] = self.response
#     #         return data
#     #     except Exception:
#     #         pass

#     # async def get_resp_data_async(
#     #     self, incl_fetch_time: bool = False, incl_response: bool = False
#     # ):
#     #     # if self.response is not None and self.response.ok:
#     #     assert isinstance(self.response, aiohttp.ClientResponse), self.response

#     #     try:
#     #         data = await self.response.json()  # type: ignore
#     #         if incl_fetch_time:
#     #             data["fetchTime"] = self.fetchTime
#     #         if incl_response:
#     #             data["response"] = self.response
#     #         return data
#     #     except Exception:
#     #         pass

#     @computed_field
#     @property
#     def is_async(self) -> bool:
#         if self.response is not None:
#             return isinstance(self.response, aiohttp.ClientResponse)
#         else:
#             from ..client.asynchronous import AsyncSchwabClient
#             from ..client.functions import getSchwabClientFactory

#             return isinstance(getSchwabClientFactory(), AsyncSchwabClient)

#     # @computed_field
#     # @property
#     # def request_method(self) -> Optional[str]:
#     #     return self.prepared_request.method

#     # @computed_field
#     # @property
#     # def request_body(self) -> Optional[Union[bytes, str]]:
#     #     return self.prepared_request.body

#     # @computed_field
#     # @property
#     # def response_text(self) -> Optional[str]:
#     #     if self.response is not None:
#     #         return self.response.text

#     # @computed_field
#     # @property
#     # def response_headers(self) -> Optional[Dict]:
#     #     if self.response is not None:
#     #         return self.response.headers._store  # type: ignore

#     # @computed_field
#     # @property
#     # def response_status_code(self) -> Optional[int]:
#     #     if self.response is not None:
#     #         return self.response.status_code

#     # @computed_field
#     # @property
#     # def response_reason(self) -> Optional[str]:
#     #     if self.response is not None:
#     #         return self.response.reason
