import arrow
from core.files import StationRealDataFile
from core.data import StationRealData
# 配置
from conf.settings import DOWNLOAD_OPTIONS
# 自定义装饰器
from util.decorators import decorator_task
from util.util import generate_key
from common.enums import TaskTypeEnum


# from core.task import

class StationRealDataCase:
    def __init__(self, utc_now: arrow.Arrow, key: int):
        self.station_realdata: StationRealData = StationRealData(utc_now)
        self.file: StationRealDataFile = None
        self.timestamp: int = utc_now.int_timestamp
        self.key = key

    def step_download(self, remote_root_path, local_root_path):
        self.file = self.station_realdata.download(remote_root_path, local_root_path, key=self.key)

    def step_to_db(self):
        self.station_realdata.to_db(self.file, key=self.key)

    @decorator_task('task_station_forecast_realdata', TaskTypeEnum.JOB_STATION)
    def todo(self, key: int):
        now_utc: arrow.Arrow = arrow.utcnow()
        remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_root_path')
        local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
        self.step_download(remote_root_path=remote_root_path, local_root_path=local_root_path, key=key)
        self.step_to_db()


def case_station_forecast_realdata():
    """
        海洋站潮位预报 case
    @return:
    """
    now_utc: arrow.Arrow = arrow.utcnow()
    key = generate_key()
    # 当前时间
    case_station = StationRealDataCase(now_utc, key)
    case_station.todo(key)
