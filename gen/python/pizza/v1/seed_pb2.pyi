from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SearchStoresInGridRequest(_message.Message):
    __slots__ = ("brand", "polygon", "cell_km", "language")
    BRAND_FIELD_NUMBER: _ClassVar[int]
    POLYGON_FIELD_NUMBER: _ClassVar[int]
    CELL_KM_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    brand: str
    polygon: Polygon
    cell_km: float
    language: str
    def __init__(self, brand: _Optional[str] = ..., polygon: _Optional[_Union[Polygon, _Mapping]] = ..., cell_km: _Optional[float] = ..., language: _Optional[str] = ...) -> None: ...

class SearchStoresInGridResponse(_message.Message):
    __slots__ = ("store",)
    STORE_FIELD_NUMBER: _ClassVar[int]
    store: Store
    def __init__(self, store: _Optional[_Union[Store, _Mapping]] = ...) -> None: ...

class Polygon(_message.Message):
    __slots__ = ("vertices",)
    VERTICES_FIELD_NUMBER: _ClassVar[int]
    vertices: _containers.RepeatedCompositeFieldContainer[LatLng]
    def __init__(self, vertices: _Optional[_Iterable[_Union[LatLng, _Mapping]]] = ...) -> None: ...

class LatLng(_message.Message):
    __slots__ = ("lat", "lng")
    LAT_FIELD_NUMBER: _ClassVar[int]
    LNG_FIELD_NUMBER: _ClassVar[int]
    lat: float
    lng: float
    def __init__(self, lat: _Optional[float] = ..., lng: _Optional[float] = ...) -> None: ...

class Store(_message.Message):
    __slots__ = ("place_id", "brand", "name", "address", "location", "official_url", "phone", "grid_cell_id")
    PLACE_ID_FIELD_NUMBER: _ClassVar[int]
    BRAND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    OFFICIAL_URL_FIELD_NUMBER: _ClassVar[int]
    PHONE_FIELD_NUMBER: _ClassVar[int]
    GRID_CELL_ID_FIELD_NUMBER: _ClassVar[int]
    place_id: str
    brand: str
    name: str
    address: str
    location: LatLng
    official_url: str
    phone: str
    grid_cell_id: str
    def __init__(self, place_id: _Optional[str] = ..., brand: _Optional[str] = ..., name: _Optional[str] = ..., address: _Optional[str] = ..., location: _Optional[_Union[LatLng, _Mapping]] = ..., official_url: _Optional[str] = ..., phone: _Optional[str] = ..., grid_cell_id: _Optional[str] = ...) -> None: ...
