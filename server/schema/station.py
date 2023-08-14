from datetime import datetime
from typing import List
from pydantic import BaseModel, Field


class StationSchema(BaseModel):
    """
        对应 tb: station_info 共享潮位站基础数据
    """
    station_code: str
    station_name: str
    lat: float
    lon: float
    is_in_common_use: int
    rid: int

    class Config:
        orm_mode = True


class StationRegionSchema(BaseModel):
    id: int
    code: str
    name: str
    lat: float
    lon: float
    sort: int
    is_in_common_use: bool


class StationSurgeJoinRegionSchema(StationRegionSchema):
    surge: float


class StationRegionSchemaList(BaseModel):
    regions_list: List[StationRegionSchema]


class MixInStationRegionSchema(BaseModel):
    station_code: str
    station_name: str
    lat: float
    lon: float
    rid: int
    val_en: str
    val_ch: str
    cid: int
    country_en: str

    class Config:
        orm_mode = True



