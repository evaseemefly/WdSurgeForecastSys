from typing import List
from pydantic import BaseModel, Field
from datetime import datetime


class CoverageFileUrlSchema(BaseModel):
    """
        栅格 file
    """
    remote_url: str

    class Config:
        orm_mode = True


class CoverageFileInfoSchema(BaseModel):
    """
        栅格 file 基础信息
    """
    # forecast_dt: datetime
    forecast_ts: int
    # issue_dt: datetime
    issue_ts: int
    task_id: int
    relative_path: str
    file_name: str
    coverage_type: int

    class Config:
        orm_mode = True


class WindVectorSchema(BaseModel):
    """
        + 23-10-08
        风场矢量数据
    """
    # 预报时间戳
    forecast_ts: int
    # 风向
    wd: float
    # 风速
    ws: float

    class Config:
        orm_mode = False
