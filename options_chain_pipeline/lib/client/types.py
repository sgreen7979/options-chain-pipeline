#!/usr/bin/env python3
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Protocol
from typing import TypedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    import logging
    import requests


class BaseStateDict(TypedDict):
    access_token: Optional[str]
    refresh_token: Optional[str]


class StateDict(BaseStateDict):
    expires_in: int
    token_type: str
    scope: str
    access_token_expires_at: float
    access_token_expires_at_date: str
    id_token: str
    refresh_token_expires_at: float
    refresh_token_expires_at_date: str


class ConfigDict(TypedDict):
    api_endpoint: str
    auth_endpoint: str
    token_endpoint: str
    refresh_enabled: bool
    refresh_token_expires_in: int
    rolling_sixty_limit: int


class CapacityLimiterProto(Protocol):
    config: ConfigDict
    client_id: str

    def _purge_request_times(self) -> int: ...
    def get_request_times(self) -> List[float]: ...
    def get_capacity(self) -> int: ...
    def has_capacity(self) -> bool: ...
    def get_next_capacity_time(self) -> float: ...
    @property
    def redis_key(self) -> str: ...
    def get_earliest_timestamp(self) -> Optional[float]: ...


class ClientProto(Protocol):
    config: ConfigDict
    client_id: str
    request_session: "requests.Session"
    _queued_tasks: List["asyncio.Task"]

    def _handle_request_error(self, response: "requests.Response") -> NoReturn: ...
    def oauth(self) -> bool: ...
    @staticmethod
    def get_logger() -> "logging.Logger": ...
    def _api_endpoint(self, endpoint: str) -> str: ...
    def validate_tokens(self) -> bool: ...

    def _create_request_headers(
        self, mode: Optional[str] = None, is_access_token_request: bool = False
    ) -> dict: ...

    def _prepare_arguments_list(self, parameter_list: List[str]) -> List[str]: ...
