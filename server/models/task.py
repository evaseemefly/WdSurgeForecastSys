from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import Integer
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX, DEFAULT_ENUM, \
    DEFAULT_FK_STR

from models.base_model import BaseMeta, IIdStrModel, IIdIntModel, IDel, IModel


class ITask(BaseMeta):
    """
        任务结果表（task_results）
    """
    __abstract__ = True
    # 关联任务 id
    task_id: Mapped[str] = mapped_column(String(8), default=DEFAULT_FK_STR)


class TaskInfoModel(IIdStrModel, IDel, IModel):
    """
        任务表（tasks）
    """
    # 任务名称
    task_name: Mapped[str] = mapped_column(String(50), default=DEFAULT_NAME)
    #  任务状态 SUCCESS|FAIL|DISCONNECT
    #          成功|失败|断开连接
    task_status: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    timestamp: Mapped[int] = mapped_column()
    # 任务种类
    # JOB_STATION
    # JOB_COVERAGE
    task_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    task_result: Mapped[str] = mapped_column(String(50), nullable=True)
    __tablename__ = 'task_infos'


class TaskJobResult(IIdIntModel, IDel, IModel, ITask):
    """
        任务结果表（task_results）
    """
    # task_status: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    job_step: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    __tablename__ = 'task_jobs'


class TaskLogs(IIdIntModel, IDel, IModel, ITask):
    """
        任务日志
    """
    log_level: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    log_content: Mapped[str] = mapped_column(String(1000))
    __tablename__ = 'task_logs'


class TaskFiles(IIdIntModel, IDel, IModel, ITask):
    """
        任务日志
    """
    file_name: Mapped[str] = mapped_column(String(200), default=DEFAULT_NAME)
    relative_path: Mapped[str] = mapped_column(String(400), default=DEFAULT_NAME)
    __tablename__ = 'task_files'
