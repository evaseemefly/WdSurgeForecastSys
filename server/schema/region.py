from datetime import datetime
from typing import List
from pydantic import BaseModel, Field


class RegionSchema(BaseModel):
    """
        行政区划信息
        id:int
        location: str
        val_en: str
        val_ch: str
        pid: int
    """
    id: int
    # location: str
    val_en: str
    val_ch: str
    pid: int

    class Config:
        orm_mode = True


class RegionFather(BaseModel):
    """
        行政区划 父节点 (包含 children)
    """
    id: int
    # location: str
    val_en: str
    val_ch: str
    pid: int
    children: List[RegionSchema]

    class Config:
        orm_mode = True


class RegionMixNumSchema(BaseModel):
    """
        + 23-04-18 地区混合了该地区全部station num 的 schema
    """
    id: int
    val_en: str
    val_ch: str
    pid: int
    count: int  # 站点总数

    class Config:
        orm_mode = True
