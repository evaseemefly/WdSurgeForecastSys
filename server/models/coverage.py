from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow
from db.db_factory import DBFactory
from common.enums import CoverageTypeEnum
from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX, DEFAULT_PATH, \
    DEFAULT_EXT, DEFAULT_ENUM

from models.base_model import BaseMeta, IIdIntModel, IDel, IModel
from models.station import IForecastTime
from models.task import ITask


class ICoverageFileModel(BaseMeta):
    __abstract__ = True
    relative_path: Mapped[str] = mapped_column(String(50), default=DEFAULT_PATH)
    file_name: Mapped[str] = mapped_column(String(100), default=DEFAULT_NAME)
    file_ext: Mapped[str] = mapped_column(String(50), default=DEFAULT_EXT)
    coverage_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)


class GeoCoverageFileModel(IDel, IIdIntModel, IForecastTime, ICoverageFileModel, IModel, ITask):
    """

    """
    __tablename__ = 'geo_coverage_file'
    pid: Mapped[int] = mapped_column(default=DEFAULT_FK)

    # def add(self, task_id: str, relative_path: str, file_name: str, forecast_dt_arrow: Arrow,
    #         coverage_type: CoverageTypeEnum, pid: int = -1):
