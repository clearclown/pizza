from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ConvertToMarkdownRequest(_message.Message):
    __slots__ = ("url", "timeout_ms", "prefer_sections")
    URL_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    PREFER_SECTIONS_FIELD_NUMBER: _ClassVar[int]
    url: str
    timeout_ms: int
    prefer_sections: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, url: _Optional[str] = ..., timeout_ms: _Optional[int] = ..., prefer_sections: _Optional[_Iterable[str]] = ...) -> None: ...

class ConvertToMarkdownResponse(_message.Message):
    __slots__ = ("doc",)
    DOC_FIELD_NUMBER: _ClassVar[int]
    doc: MarkdownDoc
    def __init__(self, doc: _Optional[_Union[MarkdownDoc, _Mapping]] = ...) -> None: ...

class MarkdownDoc(_message.Message):
    __slots__ = ("url", "markdown", "title", "metadata", "fetched_at_unix")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    URL_FIELD_NUMBER: _ClassVar[int]
    MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    FETCHED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    url: str
    markdown: str
    title: str
    metadata: _containers.ScalarMap[str, str]
    fetched_at_unix: int
    def __init__(self, url: _Optional[str] = ..., markdown: _Optional[str] = ..., title: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., fetched_at_unix: _Optional[int] = ...) -> None: ...
