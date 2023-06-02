from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow
from core.db import DbFactory
from common.enums import CoverageTypeEnum
from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX, DEFAULT_PATH, \
    DEFAULT_EXT, DEFAULT_ENUM

from model.base_model import BaseMeta, IIdIntModel, IDel, IModel
from model.station import IForecastTime, IIssueTime
from model.task import ITask

engine = DbFactory().engine
# md = MetaData(bind=engine)  # 引用MetaData
metadata = BaseMeta.metadata


class ICoverageFileModel(BaseMeta):
    __abstract__ = True
    relative_path: Mapped[str] = mapped_column(String(50), default=DEFAULT_PATH)
    file_name: Mapped[str] = mapped_column(String(100), default=DEFAULT_NAME)
    file_ext: Mapped[str] = mapped_column(String(50), default=DEFAULT_EXT)
    coverage_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)


class GeoCoverageFileModel(IDel, IIdIntModel, IForecastTime, IIssueTime, ICoverageFileModel, IModel, ITask):
    """

    """
    __tablename__ = 'geo_coverage_file'
    pid: Mapped[int] = mapped_column(default=DEFAULT_FK)

    # def add(self, task_id: str, relative_path: str, file_name: str, forecast_dt_arrow: Arrow,
    #         coverage_type: CoverageTypeEnum, pid: int = -1):


def to_migrate():
    """
        根据ORM生成数据库结构
    :return:
    """
    BaseMeta.metadata.create_all(engine)
