from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PingResp(_message.Message):
    __slots__ = ("id", "leader_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    leader_id: int
    def __init__(self, id: _Optional[int] = ..., leader_id: _Optional[int] = ...) -> None: ...

class RRaftMessage(_message.Message):
    __slots__ = ("the_bytes",)
    THE_BYTES_FIELD_NUMBER: _ClassVar[int]
    the_bytes: bytes
    def __init__(self, the_bytes: _Optional[bytes] = ...) -> None: ...

class QueryReq(_message.Message):
    __slots__ = ("query_str",)
    QUERY_STR_FIELD_NUMBER: _ClassVar[int]
    query_str: str
    def __init__(self, query_str: _Optional[str] = ...) -> None: ...

class QueryResp(_message.Message):
    __slots__ = ("error", "header", "rows_in_json")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ROWS_IN_JSON_FIELD_NUMBER: _ClassVar[int]
    error: int
    header: _containers.RepeatedScalarFieldContainer[str]
    rows_in_json: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, error: _Optional[int] = ..., header: _Optional[_Iterable[str]] = ..., rows_in_json: _Optional[_Iterable[str]] = ...) -> None: ...
