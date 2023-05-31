from datetime import datetime
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
