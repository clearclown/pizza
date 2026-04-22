from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryMegaFranchiseesRequest(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Filter
    def __init__(self, filter: _Optional[_Union[Filter, _Mapping]] = ...) -> None: ...

class QueryMegaFranchiseesResponse(_message.Message):
    __slots__ = ("megaji",)
    MEGAJI_FIELD_NUMBER: _ClassVar[int]
    megaji: Megaji
    def __init__(self, megaji: _Optional[_Union[Megaji, _Mapping]] = ...) -> None: ...

class ExportCSVRequest(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Filter
    def __init__(self, filter: _Optional[_Union[Filter, _Mapping]] = ...) -> None: ...

class ExportCSVResponse(_message.Message):
    __slots__ = ("blob",)
    BLOB_FIELD_NUMBER: _ClassVar[int]
    blob: CSVBlob
    def __init__(self, blob: _Optional[_Union[CSVBlob, _Mapping]] = ...) -> None: ...

class Filter(_message.Message):
    __slots__ = ("brand", "min_store_count", "min_confidence", "prefecture")
    BRAND_FIELD_NUMBER: _ClassVar[int]
    MIN_STORE_COUNT_FIELD_NUMBER: _ClassVar[int]
    MIN_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    PREFECTURE_FIELD_NUMBER: _ClassVar[int]
    brand: str
    min_store_count: int
    min_confidence: float
    prefecture: str
    def __init__(self, brand: _Optional[str] = ..., min_store_count: _Optional[int] = ..., min_confidence: _Optional[float] = ..., prefecture: _Optional[str] = ...) -> None: ...

class Megaji(_message.Message):
    __slots__ = ("operator_name", "store_count", "avg_confidence", "estimated_revenue_jpy", "score", "brands")
    OPERATOR_NAME_FIELD_NUMBER: _ClassVar[int]
    STORE_COUNT_FIELD_NUMBER: _ClassVar[int]
    AVG_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_REVENUE_JPY_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BRANDS_FIELD_NUMBER: _ClassVar[int]
    operator_name: str
    store_count: int
    avg_confidence: float
    estimated_revenue_jpy: float
    score: float
    brands: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, operator_name: _Optional[str] = ..., store_count: _Optional[int] = ..., avg_confidence: _Optional[float] = ..., estimated_revenue_jpy: _Optional[float] = ..., score: _Optional[float] = ..., brands: _Optional[_Iterable[str]] = ...) -> None: ...

class CSVBlob(_message.Message):
    __slots__ = ("content", "filename")
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    content: bytes
    filename: str
    def __init__(self, content: _Optional[bytes] = ..., filename: _Optional[str] = ...) -> None: ...
