from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy.orm import mapped_column, DeclarativeBase
from datetime import datetime
from arrow import Arrow
from core.db import DbFactory

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX

from common.enums import TaskTypeEnum


class BaseMeta(DeclarativeBase):
    pass


class IIdModel(BaseMeta):
    __abstract__ = True
    id: Mapped[int] = mapped_column(primary_key=True)


class IDel(BaseMeta):
    """
        软删除 抽象父类
    """
    __abstract__ = True
    is_del: Mapped[int] = mapped_column(nullable=False, default=0)


class IForecastTime(BaseMeta):
    __abstract__ = True
    forecast_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow())
    forecast_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().timestamp())


class IIssueTime(BaseMeta):
    __abstract__ = True
    issue_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow())
    issue_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().timestamp())


class IStationSurge(BaseMeta):
    __abstract__ = True
    station_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)


class StationForecastRealData(IIdModel, IDel, IForecastTime, IIssueTime, IStationSurge):
    """
        海洋预报数据
    """
    table_name_base = 'station_realdata_'

    @classmethod
    def get_split_tab_name(cls, dt_arrow: Arrow) -> str:
        """
            + 获取动态分表后的表名
        @param dt_arrow: 时间
        @return:
        """

        tab_dt_name: str = dt_arrow.format('yyyy')
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, dt_arrow: Arrow):
        """
            + 根据动态分表规则动态分表
        @param dt_arrow:
        @return:
        """
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        cls.__table__.name = tab_name

    pass
