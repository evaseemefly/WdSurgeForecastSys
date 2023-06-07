from datetime import datetime
from pydantic import BaseModel, Field


class SurgeRealDataSchema(BaseModel):
    """
        潮位实况
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
    """
    station_code: str
    forecast_dt: datetime
    issue_dt: datetime
    surge: float
    forecast_ts: int
    issue_ts: int

    class Config:
        orm_mode = True


class TideRealDataSchema(BaseModel):
    """
        天文潮 data
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
    """
    station_code: str
    gmt_realtime: datetime
    surge: float
    ts: int


class SurgeRealDataJoinStationSchema(BaseModel):
    """
        潮位实况(含station经纬度信息)
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
        lat: 经纬度
        lon: 经纬度
    """
    station_code: str
    gmt_realtime: datetime
    surge: float
    tid: int
    lat: float
    lon: float
