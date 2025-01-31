from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PingResp(_message.Message):
    __slots__ = ("id", "leader_id", "db_size_bytes", "db_location", "requests_last_minute")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    DB_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    DB_LOCATION_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_LAST_MINUTE_FIELD_NUMBER: _ClassVar[int]
    id: int
    leader_id: int
    db_size_bytes: int
    db_location: str
    requests_last_minute: int
    def __init__(self, id: _Optional[int] = ..., leader_id: _Optional[int] = ..., db_size_bytes: _Optional[int] = ..., db_location: _Optional[str] = ..., requests_last_minute: _Optional[int] = ...) -> None: ...

class RRaftMessage(_message.Message):
    __slots__ = ("the_bytes",)
    THE_BYTES_FIELD_NUMBER: _ClassVar[int]
    the_bytes: bytes
    def __init__(self, the_bytes: _Optional[bytes] = ...) -> None: ...

class QueryReq(_message.Message):
    __slots__ = ("query_str", "params_json")
    QUERY_STR_FIELD_NUMBER: _ClassVar[int]
    PARAMS_JSON_FIELD_NUMBER: _ClassVar[int]
    query_str: str
    params_json: str
    def __init__(self, query_str: _Optional[str] = ..., params_json: _Optional[str] = ...) -> None: ...

class QueryResp(_message.Message):
    __slots__ = ("err_msg", "header", "rows_in_json", "is_valid", "is_error")
    ERR_MSG_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ROWS_IN_JSON_FIELD_NUMBER: _ClassVar[int]
    IS_VALID_FIELD_NUMBER: _ClassVar[int]
    IS_ERROR_FIELD_NUMBER: _ClassVar[int]
    err_msg: str
    header: _containers.RepeatedScalarFieldContainer[str]
    rows_in_json: _containers.RepeatedScalarFieldContainer[str]
    is_valid: bool
    is_error: bool
    def __init__(self, err_msg: _Optional[str] = ..., header: _Optional[_Iterable[str]] = ..., rows_in_json: _Optional[_Iterable[str]] = ..., is_valid: bool = ..., is_error: bool = ...) -> None: ...

class LocalQueryCompleteReq(_message.Message):
    __slots__ = ("action",)
    class Action(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        COMMIT: _ClassVar[LocalQueryCompleteReq.Action]
        ABORT: _ClassVar[LocalQueryCompleteReq.Action]
    COMMIT: LocalQueryCompleteReq.Action
    ABORT: LocalQueryCompleteReq.Action
    ACTION_FIELD_NUMBER: _ClassVar[int]
    action: LocalQueryCompleteReq.Action
    def __init__(self, action: _Optional[_Union[LocalQueryCompleteReq.Action, str]] = ...) -> None: ...

class PopularRankReq(_message.Message):
    __slots__ = ("day", "temporal_granularity")
    class TemporalGranularity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DAILY: _ClassVar[PopularRankReq.TemporalGranularity]
        WEEKLY: _ClassVar[PopularRankReq.TemporalGranularity]
        MONTHLY: _ClassVar[PopularRankReq.TemporalGranularity]
    DAILY: PopularRankReq.TemporalGranularity
    WEEKLY: PopularRankReq.TemporalGranularity
    MONTHLY: PopularRankReq.TemporalGranularity
    DAY_FIELD_NUMBER: _ClassVar[int]
    TEMPORAL_GRANULARITY_FIELD_NUMBER: _ClassVar[int]
    day: str
    temporal_granularity: PopularRankReq.TemporalGranularity
    def __init__(self, day: _Optional[str] = ..., temporal_granularity: _Optional[_Union[PopularRankReq.TemporalGranularity, str]] = ...) -> None: ...
