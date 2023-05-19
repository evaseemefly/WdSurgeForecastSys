import time
import wrapt
import functools
import arrow

from common.enums import TaskStatusEnum, TaskTypeEnum, JobStepsEnum, LogLevelEnum
from core.task import TaskInfo, TaskJob


def timer_count(num: int):
    """
        方法耗时计算装饰器
        参考:
        https://zhuanlan.zhihu.com/p/68579288
        https://blog.csdn.net/weixin_40759186/article/details/86472644
    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        old_time = time.time()
        res = wrapped(*args, **kwargs)
        new_time = time.time()
        print(f'执行func:{wrapped},耗时{new_time - old_time}')
        return res

    return wrapper


def decorator_task(task_name: str, task_type: TaskTypeEnum):
    """
        task info 装饰器
    @param task_name:任务名称
    @param task_type:任务种类
    @return:
    """

    def decorate(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # utc_now: arrow.Arrow = arrow.utcnow()
            key: int = kwargs.get('key')
            task = TaskInfo(task_name, key)
            task.add(TaskStatusEnum.RUNNING)
            res = func(*args, **kwargs)
            task.update(TaskStatusEnum.SUCCESS)
            return res

    return decorate


def decorator_job(job_step: JobStepsEnum):
    def decorate(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # utc_now: arrow.Arrow = arrow.utcnow()
            # timestamp: int = kwargs.get('timestamp')
            key: int = kwargs.get('key')
            task = TaskJob(key)
            res = func(*args, **kwargs)
            task.add(job_step)
            return res

    return decorate
