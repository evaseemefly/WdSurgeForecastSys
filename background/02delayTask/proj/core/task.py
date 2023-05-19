# 任务相关模块
import arrow
from sqlalchemy import select, update
from model.task import TaskInfo, TaskLogs, TaskFiles, TaskJobResult
from common.enums import TaskStatusEnum, TaskTypeEnum, JobStepsEnum, LogLevelEnum
from common.default import NONE_ID
from core.db import DbFactory
from model.task import TaskInfo


class TaskInfo:
    def __init__(self, name: str, key: int, timestamp: int = arrow.utcnow().int_timestamp):
        # 当前时间(utc)
        self.now_utc: arrow.Arrow = arrow.utcnow()
        self.task_name: str = name
        # 任务种类 JOB_STATION | JOB_COVERAGE
        self.task_type: TaskStatusEnum = TaskStatusEnum.WAITING
        self.timestamp: int = timestamp
        self.key = key
        self.session = DbFactory().Session

        self.__task_id = key

    @property
    def task_id(self) -> int:
        return self.__task_id

    def add(self, task_status: TaskStatusEnum = TaskStatusEnum.WAITING,
            task_result: str = None):
        """
            创建一个任务
        @param task_status:
        @return:
        """
        task_info = TaskInfo(id=self.key, task_name=self.task_name, timestamp=self.timestamp, task_status=task_status,
                             task_type=task_status,
                             task_result=task_result)

        self.session.add(task_info)
        self.session.commit()
        # self.__set_task_id(task_info.id)

    def update(self, task_status: TaskStatusEnum = TaskStatusEnum.RUNNING, task_result: str = None):
        """
            更新当前 task_info id 的 status 与 result
        @param task_status:
        @param task_result:
        @return:
        """
        if self.task_id != NONE_ID:
            stmt = update(TaskInfo).where(TaskInfo.task_id == self.task_id).values(task_status=task_status,
                                                                                   task_result=task_result,
                                                                                   gmt_modify_time=arrow.utcnow())
            self.session.execute(stmt)

    def __set_task_id(self, id: int):
        """
            为 task id 赋值
        @param id:
        @return:
        """
        self.__task_id = id


class TaskFile:
    def __init__(self, relative_path: str, file_name: str, task_id: int):
        self.relative_path: str = relative_path
        self.file_name = file_name
        self.task_id = task_id
        self.session = DbFactory().Session

    def add(self):
        """
            add to tb:task_files
        @return:
        """
        task_file = TaskFiles(task_id=self.task_id, relative_path=self.relative_path, file_name=self.file_name)
        self.session.add(task_file)
        self.session.commit()
        self.session.close()

    pass


class TaskJob:
    def __init__(self, task_id: int, ):
        self.task_id = task_id
        self.session = DbFactory().Session
        pass

    def add(self, job_step: JobStepsEnum):
        job: TaskJobResult = TaskJobResult(task_id=self.task_id, job_step=job_step)
        self.session.add(job)
        self.session.commit()
        self.session.close()


class TaskLog:
    def __init__(self, task_id: int, ):
        self.task_id = task_id
        self.session = DbFactory().Session

    def add(self, log: str, log_level: LogLevelEnum):
        log: TaskLogs = TaskLogs(task_id=self.task_id, log_level=log_level, log_content=log)
        self.session.add(log)
        self.session.commit()
        self.session.close()

    pass
