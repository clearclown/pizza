from pizza.v1 import seed_pb2 as _seed_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class JudgeFranchiseTypeRequest(_message.Message):
    __slots__ = ("context",)
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    context: StoreContext
    def __init__(self, context: _Optional[_Union[StoreContext, _Mapping]] = ...) -> None: ...

class JudgeFranchiseTypeResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: JudgeResult
    def __init__(self, result: _Optional[_Union[JudgeResult, _Mapping]] = ...) -> None: ...

class BatchJudgeRequest(_message.Message):
    __slots__ = ("context",)
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    context: StoreContext
    def __init__(self, context: _Optional[_Union[StoreContext, _Mapping]] = ...) -> None: ...

class BatchJudgeResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: JudgeResult
    def __init__(self, result: _Optional[_Union[JudgeResult, _Mapping]] = ...) -> None: ...

class StoreContext(_message.Message):
    __slots__ = ("store", "markdown", "candidate_urls", "provider_hint")
    STORE_FIELD_NUMBER: _ClassVar[int]
    MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_URLS_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_HINT_FIELD_NUMBER: _ClassVar[int]
    store: _seed_pb2.Store
    markdown: str
    candidate_urls: _containers.RepeatedScalarFieldContainer[str]
    provider_hint: str
    def __init__(self, store: _Optional[_Union[_seed_pb2.Store, _Mapping]] = ..., markdown: _Optional[str] = ..., candidate_urls: _Optional[_Iterable[str]] = ..., provider_hint: _Optional[str] = ...) -> None: ...

class JudgeResult(_message.Message):
    __slots__ = ("place_id", "is_franchise", "operator_name", "store_count_estimate", "confidence", "llm_provider", "llm_model", "evidence")
    PLACE_ID_FIELD_NUMBER: _ClassVar[int]
    IS_FRANCHISE_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_NAME_FIELD_NUMBER: _ClassVar[int]
    STORE_COUNT_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    LLM_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    LLM_MODEL_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    place_id: str
    is_franchise: bool
    operator_name: str
    store_count_estimate: int
    confidence: float
    llm_provider: str
    llm_model: str
    evidence: _containers.RepeatedCompositeFieldContainer[Evidence]
    def __init__(self, place_id: _Optional[str] = ..., is_franchise: bool = ..., operator_name: _Optional[str] = ..., store_count_estimate: _Optional[int] = ..., confidence: _Optional[float] = ..., llm_provider: _Optional[str] = ..., llm_model: _Optional[str] = ..., evidence: _Optional[_Iterable[_Union[Evidence, _Mapping]]] = ...) -> None: ...

class Evidence(_message.Message):
    __slots__ = ("source_url", "snippet", "reason")
    SOURCE_URL_FIELD_NUMBER: _ClassVar[int]
    SNIPPET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_url: str
    snippet: str
    reason: str
    def __init__(self, source_url: _Optional[str] = ..., snippet: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...
