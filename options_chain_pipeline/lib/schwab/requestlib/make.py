#!/usr/bin/env python3
import json
from typing import (
    Any,
    Dict,
    Literal,
    Optional,
    Set,
    Sequence,
    Union,
)
import uuid

from pydantic import (
    BaseModel,
    computed_field,
    Field,
    field_serializer,
    field_validator,
    TypeAdapter,
)

from daily.utils.logging import get_logger

from ..api import Endpoint, Endpoints
from .priority import Priority
from .sendproto import (
    KafkaSendProtocol,
    HttpSendProtocol,
    RedisSendProtocol,
    SocketSendProtocol,
    SendProtocolType,
)
from .types import QueryVarT, QueryT

logger = get_logger(__name__, "INFO", ch=True)


class MakeRequestModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True  # type Endpoint upsets pydantic
        populate_by_name = True  # Allow using internal names
        use_enum_values = True  # If there are enums, use their values
        alias_generator = None  # No automatic alias generation
        populate_by_name = True  # Allows initialization by alias

    endpoint: Union[str, Endpoint]
    method: str = Field(default="get")
    mode: Optional[str] = Field(default=None)
    params: Optional[Dict[str, Optional[Union[str, bool, list]]]] = Field(default=None)
    data: Optional[Dict[str, str]] = Field(default=None)
    jsonr: Optional[Dict[str, str]] = Field(default=None, alias="json")
    order_details: bool = False
    incl_fetch_time: bool = False
    multi: Optional[int] = None
    incl_response: bool = False
    priority: Union[Priority, int] = Field(default=Priority.NORMAL)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()))
    capture_output: bool = Field(default=False)
    send_protocol_dict: Optional[Dict] = Field(default=None)
    headers: Dict = Field(default_factory=dict)

    @computed_field
    @property
    def send_protocol(self) -> Optional[SendProtocolType]:
        if self.send_protocol_dict is not None:
            prototype = self.send_protocol_dict["type"].upper()
            match prototype:
                case "KAFKA":
                    ta = TypeAdapter(KafkaSendProtocol)
                case "HTTP":
                    ta = TypeAdapter(HttpSendProtocol)
                case "REDIS":
                    ta = TypeAdapter(RedisSendProtocol)
                case "SOCKET":
                    ta = TypeAdapter(SocketSendProtocol)
                case _:
                    raise ValueError(
                        f"Unrecognized send_protocol type {json.dumps(self.send_protocol_dict)}"
                    )

            return ta.validate_python(self.send_protocol_dict)

    @field_validator("method")
    def validate_method(cls, value: str):
        valid_methods = {
            "get",
            "post",
            "delete",
            "head",
            "put",
            "connect",
            "options",
            "trace",
            "patch",
        }
        if value.lower() not in valid_methods:
            raise ValueError(f"Invalid method: {value}")
        return value.lower()

    @field_serializer('send_protocol', when_used="unless-none")
    def serialize_send_protocol(self, send_protocol: SendProtocolType):
        return send_protocol.model_dump_json()

    def model_dump_json(
        self,
        *,
        indent: int | None = None,
        include: Optional[Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any]] = None,
        exclude: Optional[Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any]] = None,
        context: Optional[Any] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal['none'] | Literal['warn'] | Literal['error'] = True,
        serialize_as_any: bool = False,
    ) -> str:
        return super().model_dump_json(
            indent=indent,
            include=include,
            exclude=exclude,
            context=context,
            by_alias=True,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
            serialize_as_any=serialize_as_any,
        )

    def model_dump(
        self,
        *,
        mode: str | Literal['json'] | Literal['python'] = 'python',
        include: Optional[Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any]] = None,
        exclude: Optional[Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any]] = None,
        context: Optional[Any] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal['none'] | Literal['warn'] | Literal['error'] = True,
        serialize_as_any: bool = False,
    ) -> Dict[str, Any]:
        return super().model_dump(
            mode=mode,
            include=include,
            exclude=exclude,
            context=context,
            by_alias=True,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
            serialize_as_any=serialize_as_any,
        )

    def _clean_value(self, value) -> QueryVarT:
        """
        Ensure that a given value is of type str, int, float, or
        Sequence of these types.  For compatibility with the
        `aiohttp.ClientSession` protocol.

        .. The error thrown by the aiohttp.ClientSession protocol::
            ```
            Object of type "UnionType" is not callablePylancereportCallIssue
            (type) Query = str | Mapping[str, str | int | float | Sequence[str | int | float]] | Sequence[Tuple[str, str | int | float | Sequence[str | int | float]]] | None
            ```
        :value: Any
        :rtype: PrimitiveT | Sequence[PrimitiveT]
        """
        if value is None:
            return ''
        elif isinstance(value, bool):
            return str(value)
        elif isinstance(value, (int, str, float)):
            return value
        elif isinstance(value, bytes):
            return value.decode()
        elif isinstance(value, Sequence):
            seq = []
            for v in value:
                if not isinstance(v, (int, str, float)):
                    seq.append(self._clean_value(v))
                else:
                    seq.append(v)
            return seq
        else:
            return str(value)

    def clean_params(self) -> QueryT:
        """Ensures all values in params are type str, int, or float

        Unnecessary when dealing with the requests.Session protocol,
        but aiohttp complains.

        If params is None, returns an empty string.

        :rtype::
        ```
            str | Mapping[str, int | str | float | Sequence[int | str | float]] |
            Sequence[Tuple[str, int | str | float | Sequence[int | str | float]]] |
            None
        ```
        """
        if self.params is not None:
            return {k: self._clean_value(v) for k, v in self.params.items()}
        return ''
