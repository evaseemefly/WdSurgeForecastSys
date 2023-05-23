from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow
from core.db import DbFactory

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX

from model.base_model import BaseMeta, IIdIntModel, IDel
from model.task import ITask

engine = DbFactory().engine
# md = MetaData(bind=engine)  # 引用MetaData
metadata = BaseMeta.metadata


class IForecastTime(BaseMeta):
    __abstract__ = True
    forecast_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow().date())
    forecast_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp)


class IIssueTime(BaseMeta):
    __abstract__ = True
    issue_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow().date())
    issue_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp)


class IStationSurge(BaseMeta):
    __abstract__ = True
    station_code: Mapped[str] = mapped_column(String(10), default=DEFAULT_CODE)
    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)


class StationForecastRealDataModel(IIdIntModel, IDel, IForecastTime, IIssueTime, IStationSurge, ITask):
    """
        海洋预报数据
    """
    table_name_base = 'station_realdata'
    __tablename__ = 'station_realdata_template'

    @classmethod
    def get_split_tab_name(cls, dt_arrow: Arrow) -> str:
        """
            + 获取动态分表后的表名
            按照 issue_dt 进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """

        tab_dt_name: str = dt_arrow.format('YYYY')
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, dt_arrow: Arrow):
        """
            + 根据动态分表规则动态分表
            按照 issue_dt 进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        cls.__table__.name = tab_name

    pass


def to_migrate():
    """
        根据ORM生成数据库结构
    :return:
    """
    BaseMeta.metadata.create_all(engine)
