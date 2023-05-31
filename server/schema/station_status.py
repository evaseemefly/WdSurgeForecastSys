from datetime import datetime
from pydantic import BaseModel, Field


class StationStatusSchema(BaseModel):
    """
        海洋站状态 Schema
    """
    id: int
    station_code: str
    status: int
    # gmt_realtime: datetime
    # gmt_modify_time: datetime
    tid: int
    is_del: bool
    gmt_realtime: datetime

    # TODO:[-] 23-03-10 sqlalchemy 不返回字典，而pydantic 需要字典，则需要通过设置 orm 参数通过 orm对象的属性查找而不通过字典查找!
    class Config:
        orm_mode = True


class StationStatusAndGeoInfoSchema(BaseModel):
    """
        海洋站状态 Schema
    """
    station_code: str
    status: int
    gmt_realtime: datetime
    gmt_modify_time: datetime
    is_del: bool
    lat: float
    lon: float
    rid: int
    surge: float

    # is_del: bool

    class Config:
        orm_mode = True


class StationSurgeSchema(BaseModel):
    """
        潮位实况 Schema
    """
    station_code: str
    surge: float
    tid: int
    gmt_realtime: datetime
    ts: int

    class Config:
        orm_mode = True


class StationStatusMixRegionchema(BaseModel):
    """
        潮位实况 Schema
    """
    val_ch: str
    val_en: str
    station_code: str
    gmt_realtime: datetime
    station_code: str
    id: int
    pid: int

    class Config:
        orm_mode = True
