import arrow
import xarray as xr
from typing import Optional
from common.enums import CoverageTypeEnum
from core.files import StationRealDataFile, CoverageFile
from core.data import StationRealData, CoverageData
from model.coverage import GeoCoverageFileModel
# 配置
from conf.settings import DOWNLOAD_OPTIONS
# 自定义装饰器
from util.decorators import decorator_task
from util.util import generate_key
from common.enums import TaskTypeEnum

# from core.task import

REMOTE_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('remote_root_path')
LOCAL_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('local_root_path')


class StationRealDataCase:
    """
        海洋站预报 case
        下载，
        分类存储，
        持久化保存
    """

    def __init__(self, utc_now: arrow.Arrow, key: str):
        self.station_realdata: StationRealData = StationRealData(utc_now)
        self.file: Optional[StationRealDataFile] = None
        self.timestamp: int = utc_now.int_timestamp
        self.key: str = key

    def step_download(self, remote_root_path, local_root_path, **kwargs):
        """
            将 station file 分类存储并返回 file 对象
        @param remote_root_path:网络映射盘地址
        @param local_root_path: 本地存储根目录
        @return:
        """
        # TODO:[*] 23-08-23 注意此处可能会出现 file =None 而引发的bug
        self.file = self.station_realdata.download(remote_root_path, local_root_path, key=self.key)

    def step_to_db(self):
        """
            将 station_realdata 存储至db
        @return:
        """
        self.station_realdata.to_db(self.file, key=self.key)

    @decorator_task('task_station_forecast_realdata', TaskTypeEnum.JOB_STATION)
    def todo(self, key: str):
        """
            station realdata case 执行方法
            创建 job station task
        @param key:* 必填参数，装饰器更新 task job 使用
        @return:
        """
        now_utc: arrow.Arrow = arrow.utcnow()

        self.step_download(remote_root_path=REMOTE_ROOT_PATH, local_root_path=LOCAL_ROOT_PATH, key=key)
        self.step_to_db()


class MaxSurgeCoverageCase:
    """
        最大增水场 case
    """

    def __init__(self, utc_now: arrow.Arrow, key: str):
        self.coverage: CoverageData = CoverageData(utc_now)
        self.file: Optional[CoverageFile] = None
        self.timestamp: int = utc_now.int_timestamp
        self.key: str = key
        self.__ds: xr.Dataset = None

    def step_download(self, remote_root_path: str, local_root_path: str, **kwargs):
        self.coverage.download(remote_root_path, local_root_path, key=self.key)

    def step_convert(self, local_root_path: str):
        """
            convert步骤
            step-1: step_convert_nc
            step-2: step_convert_tif
        @param local_root_path:
        @return:
        """
        ds: xr.Dataset = self.coverage.stand_2_dataset(local_root_path, key=self.key)
        nc_file: CoverageFile = self.step_convert_nc(local_root_path, ds, key=self.key)
        self.step_convert_tif(nc_file, ds, key=self.key)

    def step_convert_nc(self, local_root_path: str, ds: xr.Dataset, key: str) -> CoverageFile:
        standard_coverage_file: CoverageFile = self.coverage.convert_2_coverage(local_root_path, ds, key=self.key)
        self.coverage.to_db(key, standard_coverage_file, CoverageTypeEnum.CONVERT_COVERAGE_FILE, key=self.key)
        return standard_coverage_file

    def step_convert_tif(self, nc_file: CoverageFile, ds: xr.Dataset, key: str):
        tif_coverage_file: CoverageFile = self.coverage.convert_2_tif(ds, nc_file, key=self.key)
        self.coverage.to_db(key, tif_coverage_file, CoverageTypeEnum.CONVERT_TIF_FILE, file_ext='.tif', key=self.key)

    @decorator_task('task_station_forecast_realdata', TaskTypeEnum.JOB_COVERAGE)
    def todo(self, key: str):
        self.step_download(REMOTE_ROOT_PATH, LOCAL_ROOT_PATH)
        self.step_convert(LOCAL_ROOT_PATH)


def case_timer_station_forecast_realdata():
    """
        海洋站潮位预报 case
        此方法为调用 station realdata 的入口方法，根据当前时间执行 下载 -> 分类存储 -> to db 操作
    @return:
    """
    now_utc: arrow.Arrow = arrow.utcnow()
    key = generate_key()
    # 当前时间
    case_station = StationRealDataCase(now_utc, key)
    case_station.todo(key=key)


def case_timer_maxsurge_coverage():
    """
        定时下载最大增水场并标准化入库
        取消了 case 实例中的 todo 方法
    @return:
    """
    now_utc: arrow.Arrow = arrow.utcnow()
    task_key: str = generate_key()
    maxsurge_coverage = MaxSurgeCoverageCase(now_utc, task_key)
    maxsurge_coverage.todo(key=task_key)
