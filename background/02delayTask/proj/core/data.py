import abc
import datetime
import pathlib

import xarray
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from arrow import Arrow
import arrow
import shutil
from typing import Optional, List, Dict

from pandas import Series
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray

# ftp 库
import ftplib

from common.default import DEFAULT_FK_STR
from conf._privacy import FTP_LIST
from core.db import DbFactory
from core.files import StationRealDataFile, CoverageFile
from conf.settings import DOWNLOAD_OPTIONS
from core.task import TaskFile
from model.mid_models import FtpClientMidModel
from model.station import StationForecastRealDataModel
from model.coverage import GeoCoverageFileModel
from util.decorators import decorator_job
from util.util import get_relative_path, FtpFactory
from common.enums import JobStepsEnum, CoverageTypeEnum
from common.comm_dicts import station_code_dicts


class IFileInfo:

    def __init__(self, now: Arrow):
        self.now: Arrow = now
        self.session = DbFactory().Session

    def get_nearly_forecast_dt(self) -> Arrow:
        """
            预报文件生成的时间
                预报时间: 00Z  发布时间 09-8=01 15-01
                        12Z          23-8=15 01-15
            此部分与 StationRealData 中重复
        :return:
        """
        # 2023-05-22T07:58:43.111279+00:00
        # 对应的本地时间为 2023-05-22T15:58:43.111279
        now_utc: Arrow = self.now
        stamp_hour = self.now.time().hour
        forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 12, 0)
        # 判断是 00Z 还是 12Z
        # local time : [9,23)
        if stamp_hour >= 1 and stamp_hour < 15:
            now_utc = now_utc.shift(days=-1)
            # [1,15]
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 12, 0)
        # local time : [23,9)
        elif stamp_hour >= 15:
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 0, 0)
        # lcoal time: [8,9)
        elif stamp_hour < 1:
            now_utc = now_utc.shift(hours=-1)
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 0, 0)
        return forecast_dt

    @abc.abstractmethod
    def get_file_name(self):
        pass

    @abc.abstractmethod
    def download(self, dir_path: str, copy_dir_path: str) -> StationRealDataFile:
        pass


class IFtpInfo:
    """
        + 23-09-20 新加入了ftp下载 info
    """

    def __init__(self, root_path: str, remote_path: str, now_arrow: Arrow):
        self.root_path = root_path
        self.remote_path = remote_path
        # self.file_name = file_name
        # utc 的当前时间
        self.now_arrow: Arrow = now_arrow
        self.session = DbFactory().Session

    def get_nearly_forecast_dt(self) -> Arrow:
        """
            风场生成逻辑:
            以 nwp 为例:
                        nwp_high_res_wind_2023091600.nc | 23/09/16 17:52 [16日 当日 18 - 次日 05] local
                        nwp_high_res_wind_2023091612.nc | 23/09/17 05:53 [17日 06 - 17日 18]  local
                        ---
                        nwp_high_res_wind_2023091600.nc | 23/09/16 17:52 [16日 当日 18 - 次日 05] local
                        nwp_high_res_wind_2023091612.nc | 23/09/17 05:53 [17日 06 - 17日 18]  local
                        utc : 00时 -> 17-8 =9
                        utc : 12时 -> 5-8  =21
                        23,0,1,2,3,4,5 -8 =
            获取预报的utc时间

        @return: 获取当前 self.now_arrow(utc)对应的预报时间(utc)
        """
        # 小时
        stamp_hour = self.now_arrow.time().hour
        now_utc: Arrow = self.now_arrow
        # 预报基准时间(utc)
        # 00,12
        forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 0, 0)
        # 16 6 -> 18
        if stamp_hour >= 9 and stamp_hour <= 21:
            # 预报时间是 前一日 12时
            # 15
            forecast_dt = forecast_dt
        elif stamp_hour < 9:
            # [0-5]
            forecast_dt = forecast_dt.shift(hours=-12)
        elif stamp_hour > 21:
            forecast_dt = forecast_dt.shift(hours=12)
        return forecast_dt

    @abc.abstractmethod
    def get_file_name(self) -> str:
        pass


class WindCoverageData(IFtpInfo):
    """
        风场栅格数据
    """
    # 经纬度范围
    lat_range: List[float] = [16.1, 41.0]
    lon_range: List[float] = [105.0, 126.9]

    def __init__(self, root_path: str, remote_path: str, now_arrow: Arrow):
        super(WindCoverageData, self).__init__(root_path, remote_path, now_arrow)

    def get_file_name(self) -> str:
        """
            对应的文件名称
        @return:
        """
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        date_name: str = forecast_dt.format("YYYYMMDDHH")
        file_name: str = f'nwp_high_res_wind_{date_name}.nc'
        return file_name

    def get_relative_path(self) -> str:
        """
            获取相对路径
            * 统一根据 get_nearly_forecast_dt -> now_arrow 获取相对路径
        @return:
        """
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        return get_relative_path(forecast_dt)

    def get_store_path(self) -> str:
        """
            获取存储路径
            root_path/relative_path
            * 统一根据 get_nearly_forecast_dt -> now_arrow 获取相对路径 relative_path
        @return:
        """
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        # 本地存储的相对路径(相对于root路径)
        relative_path: str = self.get_relative_path()
        store_path: str = str(pathlib.Path(self.root_path) / relative_path)
        return store_path

    def get_local_full_path(self) -> str:
        """
            下载到本地的文件全路径
        @return:
        """
        return str(pathlib.Path(self.get_store_path()) / self.get_file_name())

    def standard_ds(self, dir_path: str) -> bool:
        """
            将栅格文件标准化
        @param dir_path:
        @return:
        """
        return False

    def download(self, ftp_client: FtpFactory, local_path: str, relative_path: str, key: str, pid=-1,
                 file_ext: str = '.nc') -> \
            Optional[CoverageFile]:
        """
            下载并返回下载后的 coverage file
        @return:
        """

        # local_path: str = ftp_opt.get('LOCAL_PATH')
        # relative_path: str = ftp_opt.get('RELATIVE_PATH')
        #
        target_file_name: str = self.get_file_name()
        local_copy_full_path: str = self.get_local_full_path()
        is_ok = True
        # TODO:[*] 23-09-26 FileExistsError: [WinError 183] 当文件已存在时，无法创建该文件。: 'E:\\05DATA\\06wind'
        # 'E:\\05DATA\\06wind\\2023\\09\\nwp_high_res_wind_2023092412.nc'
        if pathlib.Path(local_copy_full_path).parent.exists():
            pass
        else:
            pathlib.Path(local_copy_full_path).parent.mkdir(parents=True)
        is_ok: bool = ftp_client.down_load_file_bycwd(local_copy_full_path, relative_path, target_file_name)
        # is_ok: bool = True
        download_file: Optional[CoverageFile] = None
        if is_ok:
            download_file: CoverageFile = CoverageFile(self.root_path, self.get_relative_path(), target_file_name)
            # TODO:[-] 23-09-25 to db
            if download_file is not None:
                coverage_file_model: GeoCoverageFileModel = GeoCoverageFileModel(task_id=key,
                                                                                 relative_path=download_file.relative_path,
                                                                                 file_name=download_file.file_name,
                                                                                 coverage_type=CoverageTypeEnum.NWP_SOURCE_COVERAGE_FILE.value,
                                                                                 forecast_dt=download_file.forecast_dt_start.datetime,
                                                                                 forecast_ts=download_file.forecast_dt_start.int_timestamp,
                                                                                 issue_dt=download_file.forecast_dt_start.datetime,
                                                                                 issue_ts=download_file.forecast_dt_start.int_timestamp,
                                                                                 file_ext=file_ext,
                                                                                 pid=pid
                                                                                 )
                self.session.add(coverage_file_model)
                self.session.commit()
                self.session.close()

        return download_file

    def split_2_coverage(self, coverage_file: CoverageFile, key: str, pid=-1, file_ext: str = '.nc', ) -> Optional[
        CoverageFile]:
        """
            按需裁剪风场文件
            将下载后的风场文件切割并存储为新的nc文件
        @return:
        """
        coverage_full_path: str = coverage_file.full_path
        # TODO:[*] 23-09-26 暂时修改为 win 路径
        # coverage_full_path = r'../data/nwp_high_res_wind_2023092512.nc'
        # coverage_full_path = r'E:/05DATA/06wind/2023/09/nwp_high_res_wind_2023092512.nc'
        root_path: str = coverage_file.root_path
        relative_path: str = coverage_file.relative_path
        file_name: str = coverage_file.file_name_only
        saved_coverage_file: Optional[CoverageFile] = None
        if pathlib.Path(coverage_full_path).exists():
            # TODO:[-] 由于 win 路径的转义字符的问题造成读取bug
            # nwp_high_res_wind_2023092512.nc
            # '/data/local/2023/09/nwp_high_res_wind_2023092512.nc'
            # 在docker中出现:AttributeError: 'EntryPoints' object has no attribute 'get'
            # docker xarray 版本: '0.20.2'
            #
            ds_xr: xarray.Dataset = xarray.open_dataset(coverage_full_path)
            # 获取经纬度的范围
            min_lon: float = min(self.lon_range)
            max_lon: float = max(self.lon_range)
            min_lat: float = min(self.lat_range)
            max_lat: float = max(self.lat_range)
            mask_lon = (ds_xr.lon >= min_lon) & (ds_xr.lon <= max_lon)
            mask_lat = (ds_xr.lat >= min_lat) & (ds_xr.lat <= max_lat)
            # 根据经纬度范围裁剪生成新的 dataset
            cropped_ds = ds_xr.where(mask_lon & mask_lat, drop=True)
            save_name: str = f'{file_name}_output.nc'
            save_full_path: str = str(pathlib.Path(root_path) / relative_path / save_name)
            try:
                # 已解决
                cropped_ds.to_netcdf(save_full_path, format='NETCDF4', mode='w')
                saved_coverage_file = CoverageFile(root_path, relative_path, save_name)
                if saved_coverage_file is not None:
                    # TODO:[-] 23-09-25 to db
                    coverage_file_model: GeoCoverageFileModel = GeoCoverageFileModel(task_id=key,
                                                                                     relative_path=relative_path,
                                                                                     file_name=saved_coverage_file.file_name,
                                                                                     coverage_type=CoverageTypeEnum.NWP_SPLIT_COVERAGE_FILE.value,
                                                                                     forecast_dt=saved_coverage_file.forecast_dt_start.datetime,
                                                                                     forecast_ts=saved_coverage_file.forecast_dt_start.int_timestamp,
                                                                                     issue_dt=saved_coverage_file.forecast_dt_start.datetime,
                                                                                     issue_ts=saved_coverage_file.forecast_dt_start.int_timestamp,
                                                                                     file_ext=file_ext,
                                                                                     pid=pid
                                                                                     )
                    self.session.add(coverage_file_model)
                    self.session.commit()
                    self.session.close()
            except Exception as ex:
                print(f'切分原始风场文件错误:{ex.args}')
        return saved_coverage_file

    def convert_2_tif(self, coverage_file: CoverageFile, field_name: str, key: str, pid: int = -1, file_ext='.tif'):
        """
            将 nc -> 提取为 tif
        @param ds:
        @param field_name: 提取的字段名称
        @return:
        """
        coord_time: str = 'time'
        # TODO:[*] 23-10-09 线上部署时出错
        print(f'读取:{coverage_file.full_path}并切分为tif ing')
        # entrypoints = entry_points().get("xarray.backends", ())
        # AttributeError: 'EntryPoints' object has no attribute 'get'
        ds = xarray.open_dataset(coverage_file.full_path)
        # 从 ds 中提取所有的时间维度
        for index, temp_dt64 in enumerate(ds.coords[field_name].values):
            # <xarray.DataArray 'time' ()>
            # array('2023-09-25T12:00:00.000000000', dtype='datetime64[ns]')
            # Coordinates:
            #     time     datetime64[ns] 2023-09-25T12:00:00
            # temp_dt: datetime.datetime = temp_dt64.astype(datetime.datetime)
            # pd.to_datetime(temp_dt).to_pydatetime()
            # dataarray datetime64 -> arrow
            temp_dt_arrow: Arrow = arrow.get(pd.to_datetime(temp_dt64))
            temp_split_ds: xr.Dataset = ds.isel(time=0)
            # tif 栅格文件
            tif_coverage_file: CoverageFile = self._loop_2_tif(coverage_file, temp_split_ds, index, temp_dt_arrow,
                                                               field_name)
            # TODO:[-] 23-09-25 to db
            if tif_coverage_file is not None:
                coverage_file_model: GeoCoverageFileModel = GeoCoverageFileModel(task_id=key,
                                                                                 relative_path=tif_coverage_file.relative_path,
                                                                                 file_name=tif_coverage_file.file_name,
                                                                                 coverage_type=CoverageTypeEnum.NWP_TIF_FILE.value,
                                                                                 forecast_dt=temp_dt_arrow.datetime,
                                                                                 forecast_ts=temp_dt_arrow.int_timestamp,
                                                                                 issue_dt=tif_coverage_file.forecast_dt_start.datetime,
                                                                                 issue_ts=tif_coverage_file.forecast_dt_start.int_timestamp,
                                                                                 file_ext=file_ext,
                                                                                 pid=pid
                                                                                 )
                self.session.add(coverage_file_model)
                self.session.commit()
                self.session.close()
        pass

    def _loop_2_tif(self, coverage_file: CoverageFile, ds: xr.Dataset, index: int, forecast_dt: Arrow,
                    field_name: str) -> Optional[CoverageFile]:
        ds.rio.write_crs("epsg:4326", inplace=True)
        ds_field_ds = xr.Dataset({field_name: ds[field_name]})
        ds = ds.rio.set_spatial_dims('lat', 'lon')
        ds = ds.rename_dims({'lat': 'latitude', 'lon': 'longitude'})
        # 存储为 geotiff
        geotiff_file_name: str = f'{coverage_file.file_name_only}_{index}.tif'
        dir_path: pathlib.Path = pathlib.Path(
            coverage_file.root_path) / coverage_file.relative_path
        output_file: str = str(dir_path / geotiff_file_name)
        output_file: CoverageFile = CoverageFile(coverage_file.root_path, coverage_file.relative_path,
                                                 geotiff_file_name)
        # 若不纯在指定目录则创建
        if dir_path.exists() is False:
            dir_path.mkdir()
        try:
            ds.rio.to_raster(output_file.full_path)
        except Exception as ex:
            print(f'生成文件:{output_file}出错!')
        return output_file


class StationRealData(IFileInfo):
    def __init__(self, now: Arrow):
        super(StationRealData, self).__init__(now)
        # self.now: Arrow = now
        # self.session = DbFactory().Session

    @decorator_job(JobStepsEnum.DOWNLOAD_STATION)
    def download(self, dir_path: str, copy_dir_path: str, key: str) -> Optional[StationRealDataFile]:
        """
            根据 self.now 进行文件下载
        @param dir_path: 原始路径
        @param copy_dir_path: 存储路径
        @return: StationRealDataFile 海洋站预报潮位文件
        """

        now_year_str: str = self.now.format('YYYY')
        now_month_str: str = self.now.format('MM')
        file_name_str: str = self.get_file_name()
        # TODO:[-] 23-09-06 '/data/remote/NMF_TRN_OSTZSS_CSDT_2023090512_168h_SS_staSurge.txt'
        source_full_path: str = str(pathlib.Path(dir_path) / file_name_str)
        copy_path: str = f'{copy_dir_path}/{now_year_str}/{now_month_str}'
        relative_path: str = f'{now_year_str}/{now_month_str}'
        copy_full_path: str = str(pathlib.Path(copy_path) / file_name_str)
        # TODO:[*] 23-05-22 加入判断
        # '/data/remote/NMF_TRN_OSTZSS_CSDT_2023052212_168h_SS_staSurge.txt'
        # NMF_TRN_OSTZSS_CSDT_2023052212_168h_SS_staSurge.txt
        # NMF_TRN_OSTZSS_CSDT_2023052112_168h_SS_staSurge.txt
        if pathlib.Path(source_full_path).is_file():
            if pathlib.Path(copy_path).exists():
                pass
            else:
                pathlib.Path(copy_path).mkdir(parents=True, exist_ok=False)
            shutil.copyfile(source_full_path, copy_full_path)
            task_file = TaskFile(copy_path, file_name_str, key)
            # TODO:[*] 23-05-22
            # sqlalchemy.exc.IntegrityError: (MySQLdb._exceptions.IntegrityError) (1062, "Duplicate entry '-1' for key 'PRIMARY'")
            # [SQL: INSERT INTO task_files (file_name, relative_path, id, is_del, gmt_create_time, gmt_modify_time, task_id) VALUES (%s, %s, %s, %s, %s, %s, %s)]
            # [parameters: ('NMF_TRN_OSTZSS_CSDT_2023052112_168h_SS_staSurge.txt', 'D:\\05DATA\\NGINX_PATH\\TIDE\\local/2023/05', -1, 0, datetime.datetime(2023, 5, 22, 12, 54, 56, 896541), datetime.datetime(2023, 5, 22, 12, 54, 56, 896541), '1b51de5c')]
            # (Background on this error at: https://sqlalche.me/e/20/gkpj)
            task_file.add()
            return StationRealDataFile(copy_dir_path, relative_path, file_name_str)
        else:
            return None

    def __get_nearly_station_surge_list(self):
        """
            根据 self.now 获取临近的时间的文件路径，并读取该文件获取对应的站点预报集合
        :return:
        """
        # file_name: NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_staSurge.txt

    def __check_exist_tab(self, tab_name: str) -> bool:
        """
            判断指定表是否存在
        @param tab_name:
        @return:
        """
        is_exist = False
        auto_base = automap_base()
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        auto_base.prepare(engine, reflect=True)
        list_tabs = auto_base.classes
        if tab_name in list_tabs:
            is_exist = True
        return is_exist

    def __create_realdata_tab(self, tab_name: str) -> bool:
        is_ok = False
        meta_data = MetaData()
        now_utc: Arrow = arrow.utcnow()
        Table(tab_name, meta_data, Column('id', Integer, primary_key=True),
              Column('is_del', TINYINT(1), nullable=False, server_default=text("'0'"), default=0),
              Column('station_code', VARCHAR(200), nullable=False, index=True),
              Column('surge', Float, nullable=False),
              Column('task_id', VARCHAR(8), nullable=False, index=True),
              Column('forecast_ts', Integer, nullable=False, default=now_utc.int_timestamp),
              Column('issue_ts', Integer, nullable=False, default=now_utc.int_timestamp),
              Column('forecast_dt', DATETIME(fsp=6), default=now_utc.datetime),
              Column('issue_dt', DATETIME(fsp=6), default=now_utc.datetime))
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        with engine.connect() as conn:
            # result_proxy = conn.execute(sql_str)
            # result = result_proxy.fetchall()
            try:
                meta_data.create_all(engine)
                is_ok = True
            except Exception as ex:
                print(ex.args)
        return is_ok

    def get_file_name(self):
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        date_str: str = forecast_dt.format("YYYYMMDDHH")
        file_name: str = f'NMF_TRN_OSTZSS_CSDT_{date_str}_168h_SS_staSurge.txt'
        return file_name

    @decorator_job(JobStepsEnum.STORE_DB_STATION)
    def to_db(self, station_file: StationRealDataFile, key: int):
        """
            持久化保存
            将 station_file 中的站点潮位数据写入 db
        @param station_file:
        @param key: * 必填参数，装饰器更新 task job 使用
        @return:
        """
        dict_station_list: Dict[str, Series] = station_file.get_station_realdata_list()

        for temp_key in dict_station_list:
            temp_forecast_start_dt: Arrow = station_file.forecast_dt_start
            temp_code: str = temp_key
            surge_list: Series = dict_station_list[temp_code]
            StationForecastRealDataModel.set_split_tab_name(self.get_nearly_forecast_dt())
            # 判断分表名称
            tab_name: str = StationForecastRealDataModel.get_split_tab_name(self.get_nearly_forecast_dt())
            if self.__check_exist_tab(tab_name) == False:
                self.__create_realdata_tab(tab_name)
            # TODO:[-] 23-09-19 注意温带风暴潮会提前输出一天的预报，需要跳过1天前的数据[25:]
            # TODO:[-] 23-09-21 若168个时刻是 ec;192个时刻是中心风场
            if len(surge_list) == 168:
                split_surge_list = surge_list
            else:
                split_surge_list = surge_list[25:]
            for index, temp_surge in enumerate(split_surge_list):
                temp_dt: Arrow = temp_forecast_start_dt.shift(hours=index)

                temp_station_model: StationForecastRealDataModel = StationForecastRealDataModel(surge=temp_surge,
                                                                                                station_code=temp_code,
                                                                                                forecast_dt=temp_dt.datetime,
                                                                                                forecast_ts=temp_dt.int_timestamp,
                                                                                                issue_dt=self.get_nearly_forecast_dt().datetime,
                                                                                                issue_ts=self.get_nearly_forecast_dt().int_timestamp,
                                                                                                task_id=key
                                                                                                )

                self.session.add(temp_station_model)
            self.session.commit()
            self.session.close()
            pass


class CoverageData(IFileInfo):
    #
    root_path: str = r''

    def __init__(self, now: Arrow):
        super(CoverageData, self).__init__(now)
        # self.now: Arrow = now
        # self.session = DbFactory().Session

    def get_nearly_forecast_dt(self) -> Arrow:
        """
            预报文件生成的时间
                预报时间: 00Z  发布时间 09-8=01 15-01
                        12Z          23-8=15 01-15
            此部分与 StationRealData 中重复
        :return:
        """
        # 2023-05-22T07:58:43.111279+00:00
        # 对应的本地时间为 2023-05-22T15:58:43.111279
        now_utc: Arrow = self.now
        stamp_hour = self.now.time().hour
        forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 12, 0)
        # 判断是 00Z 还是 12Z
        # local time : [9,23)
        if stamp_hour >= 1 and stamp_hour < 15:
            now_utc = now_utc.shift(days=-1)
            # [1,15]
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 12, 0)
        # local time : [23,9)
        elif stamp_hour >= 15:
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 0, 0)
        # lcoal time: [8,9)
        elif stamp_hour < 1:
            now_utc = now_utc.shift(hours=-1)
            forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 0, 0)
        return forecast_dt

    @decorator_job(JobStepsEnum.DOWNLOAD_COVERAGE)
    def download(self, dir_path: str, copy_dir_path: str, key: str) -> Optional[CoverageFile]:
        """
            根据 self.now 进行文件下载
        @param key: task_id
        @param dir_path: 原始路径
        @param copy_dir_path: 存储路径
        @return: StationRealDataFile 海洋站预报潮位文件
        """
        # TODO:[-] 23-09-01 注意 copy path 需要与 convert 的相对路径一致
        nearly_forecast_arrow: Arrow = self.get_nearly_forecast_dt()
        # now_year_str: str = self.now.format('YYYY')
        # now_month_str: str = self.now.format('MM')
        now_year_str: str = nearly_forecast_arrow.format('YYYY')
        now_month_str: str = nearly_forecast_arrow.format('MM')
        file_name_str: str = self.get_file_name('txt')
        source_full_path: str = str(pathlib.Path(dir_path) / file_name_str)
        copy_path: str = f'{copy_dir_path}/{now_year_str}/{now_month_str}'
        relative_path: str = f'{now_year_str}/{now_month_str}'
        copy_full_path: str = str(pathlib.Path(copy_path) / file_name_str)
        # TODO:[*] 23-05-22 加入判断
        # '/data/remote/NMF_TRN_OSTZSS_CSDT_2023052212_168h_SS_staSurge.txt'
        # NMF_TRN_OSTZSS_CSDT_2023052212_168h_SS_staSurge.txt
        # NMF_TRN_OSTZSS_CSDT_2023052112_168h_SS_staSurge.txt
        if pathlib.Path(source_full_path).is_file():
            if pathlib.Path(copy_path).exists():
                pass
            else:
                pathlib.Path(copy_path).mkdir(parents=True, exist_ok=False)
            shutil.copyfile(source_full_path, copy_full_path)
            task_file = TaskFile(copy_path, file_name_str, key)
            task_file.add()
            return CoverageFile(copy_path, get_relative_path(self.get_nearly_forecast_dt()), file_name_str)
        else:
            return None

    def get_file_name(self, file_ext: str) -> str:
        """
            根据当前时间获取最近的预报时刻生成对应的文件名称
        @param file_ext: 文件后缀
        @return:
        """
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        date_str: str = forecast_dt.format("YYYYMMDDHH")
        file_name: str = f'NMF_TRN_OSTZSS_CSDT_{date_str}_168h_SS_maxSurge.{file_ext}'
        return file_name

    def standard_ds(self, dir_path: str) -> xr.Dataset:
        root_path: str = DOWNLOAD_OPTIONS.get('remote_root_path')
        relative_path: str = get_relative_path(
            self.get_nearly_forecast_dt())
        file_name_nc: str = self.get_file_name('nc')
        file_full_path: str = str(pathlib.Path(dir_path) / relative_path / file_name_nc)

        # 将 txt => nc
        # step-1: 判断文件是否存在
        if pathlib.Path(file_full_path).exists():
            # step-2: 读取txt文件并加载至 DataFrame 中
            with open(file_full_path, 'rb') as f:
                # pandas.core.frame.DataFrame
                data: pd.DataFrame = pd.read_csv(f, encoding='gbk', sep='\s+', header=None,
                                                 infer_datetime_format=False)
                # 此处需要加入对原矩阵的转置操作
                data_T: pd.DataFrame = data.transpose()
                # step-3: 生成经纬度集合
                # 定义经纬度数组
                # 注意经纬度网格的 长宽 无问题，但是范围有问题
                # 220
                lon = np.arange(105, 127, 0.1)
                # 250
                lat = np.arange(16, 41, 0.1)
                # step-4: 创建当前定义的经纬度坐标系的新的DataFrame
                da = xr.DataArray(data_T, coords=[lat, lon], dims=['lat', 'lon'])
                # step-5: DataFrame => Dataset
                ds: xr.Dataset = xr.Dataset({'max_surge': da})
                # step-6-1: 对 lat 进行倒叙排列——[0,0] 位置的 lat是max
                ds_sorted_y: xr.Dataset = ds.sortby('lat', ascending=False)
                # step-6-2: 对经纬度信息进行标准化
                ds_sorted_y['lat'].attrs['axis'] = 'Y'
                ds_sorted_y['lat'].attrs['units'] = 'degrees_north'
                ds_sorted_y['lat'].attrs['long_name'] = 'latitude'
                ds_sorted_y['lat'].attrs['standard_name'] = 'latitude'
                ds_sorted_y['lon'].attrs['axis'] = 'X'
                ds_sorted_y['lon'].attrs['units'] = 'degrees_east'
                ds_sorted_y['lon'].attrs['long_name'] = 'longitude'
                ds_sorted_y['lon'].attrs['standard_name'] = 'longitude'
                # step-6-3: 定义crs
                ds_sorted_y = ds_sorted_y.rio.write_crs("epsg:4326", inplace=True)
                # step-7: 转存为新的 nc文件，并返回文件
                nc_full_path: str = str(pathlib.Path(dir_path) / relative_path / file_name_nc)
                ds_sorted_y.to_netcdf(nc_full_path, format='NETCDF4', mode='w')
                return CoverageFile(dir_path, relative_path, file_name_nc)

            pass
        else:
            return None
            pass

    @decorator_job(JobStepsEnum.STANDARD_COVERAGE)
    def stand_2_dataset(self, dir_path: str, key: str) -> Optional[xr.Dataset]:
        """
            将 dir_path 为根目录根据当前的时间获取对应的文件并标准化后返回 Dataset
            並不存储为新文件
            读取时对于 utc 31d 会从 31日目录中读取
        @param dir_path:
        @return:
        """
        # TODO:[*] 23-09-01 注意此处的相对路径为 23/08 此路径下并为存储对应文件
        # '/data/local/2023/08/NMF_TRN_OSTZSS_CSDT_2023083112_168h_SS_maxSurge.txt'
        relative_path: str = get_relative_path(
            self.get_nearly_forecast_dt())
        file_name_nc: str = self.get_file_name('txt')
        file_full_path: str = str(pathlib.Path(dir_path) / relative_path / file_name_nc)

        # 将 txt => nc
        # step-1: 判断文件是否存在
        if pathlib.Path(file_full_path).exists():
            # step-2: 读取txt文件并加载至 DataFrame 中
            with open(file_full_path, 'rb') as f:
                # pandas.core.frame.DataFrame
                data: pd.DataFrame = pd.read_csv(f, encoding='gbk', sep='\s+', header=None,
                                                 infer_datetime_format=False)
                # 此处需要加入对原矩阵的转置操作
                data_T: pd.DataFrame = data.transpose()
                data_T = data_T[data_T != 999.0]
                # step-3: 生成经纬度集合
                # 定义经纬度数组
                # 注意经纬度网格的 长宽 无问题，但是范围有问题
                # 220
                lon = np.arange(105, 127, 0.1)
                # 250
                lat = np.arange(41, 16, -0.1)
                # step-4: 创建当前定义的经纬度坐标系的新的DataFrame
                da = xr.DataArray(data_T, coords=[lat, lon], dims=['lat', 'lon'])
                # step-5: DataFrame => Dataset
                ds: xr.Dataset = xr.Dataset({'max_surge': da})
                # step-6-1: 对 lat 进行倒叙排列——[0,0] 位置的 lat是max
                ds_sorted_y: xr.Dataset = ds.sortby('lat', ascending=False)
                # step-6-2: 对经纬度信息进行标准化
                ds_sorted_y['lat'].attrs['axis'] = 'Y'
                ds_sorted_y['lat'].attrs['units'] = 'degrees_north'
                ds_sorted_y['lat'].attrs['long_name'] = 'latitude'
                ds_sorted_y['lat'].attrs['standard_name'] = 'latitude'
                ds_sorted_y['lon'].attrs['axis'] = 'X'
                ds_sorted_y['lon'].attrs['units'] = 'degrees_east'
                ds_sorted_y['lon'].attrs['long_name'] = 'longitude'
                ds_sorted_y['lon'].attrs['standard_name'] = 'longitude'
                # step-6-3: 定义crs
                ds_sorted_y = ds_sorted_y.rio.write_crs("epsg:4326", inplace=True)
                return ds_sorted_y
            pass
        else:
            return None
            pass

    @decorator_job(JobStepsEnum.CONVERT_COVERAGE_NC)
    def convert_2_coverage(self, dir_path: str, ds: xr.Dataset, key: str) -> Optional[CoverageFile]:
        root_path: str = DOWNLOAD_OPTIONS.get('remote_root_path')
        relative_path: str = get_relative_path(
            self.get_nearly_forecast_dt())
        file_name_nc: str = self.get_file_name('nc')
        file_full_path: str = str(pathlib.Path(dir_path) / relative_path / file_name_nc)
        if ds is not None:
            # step-7: 转存为新的 nc文件，并返回文件
            nc_full_path: str = str(pathlib.Path(dir_path) / relative_path / file_name_nc)
            # TODO:[-] 23-05-29 ValueError: cannot read or write netCDF files without netCDF4-python or scipy installed
            # docker 环境:
            # xarray: 0.20.2
            # pandas: 1.3.5
            # numpy: 1.21.5
            # scipy: None
            # netCDF4: None
            # pydap: None
            # h5netcdf: None
            # h5py: None
            # Nio: None
            # zarr: None
            # -----
            # xarray: 0.17.0
            # pandas: 1.2.3
            # numpy: 1.20.2
            # scipy: 1.6.2
            # netCDF4: 1.5.6
            # pydap: None
            # h5netcdf: None
            # h5py: None
            # Nio: None
            # zarr: None
            ds.to_netcdf(nc_full_path, format='NETCDF4', mode='w')
            return CoverageFile(dir_path, relative_path, file_name_nc)
        else:
            return None

    @decorator_job(JobStepsEnum.CONVERT_COVERAGE_TIF)
    def convert_2_tif(self, ds: xr.Dataset, nc_file: CoverageFile, key: str) -> CoverageFile:
        """
            将 转换后的 nc -> tif
        @param ds:
        @param nc_file: nc文件
        @return:
        """
        file_name: str = f'{nc_file.file_name_only}.tif'
        tif_full_path: str = str(pathlib.Path(nc_file.root_path) / nc_file.relative_path / file_name)
        ds.rio.to_raster(tif_full_path)
        return CoverageFile(nc_file.root_path, nc_file.relative_path, file_name)

    @decorator_job(JobStepsEnum.STORE_DB_COVERAGE)
    def to_db(self, task_id: str, coverage_file: CoverageFile, coverage_type: CoverageTypeEnum, pid=-1, file_ext='.nc',
              key: str = DEFAULT_FK_STR):
        """
            记录当前 coverage_file to db
        @param task_id:
        @param coverage_file:
        @param coverage_type:
        @param pid:
        @param file_ext: 文件后缀 (默认 =.nc)
        @return:
        """
        if coverage_file is not None:
            coverage_file_model: GeoCoverageFileModel = GeoCoverageFileModel(task_id=task_id,
                                                                             relative_path=coverage_file.relative_path,
                                                                             file_name=coverage_file.file_name,
                                                                             coverage_type=coverage_type.value,
                                                                             forecast_dt=coverage_file.forecast_dt_start.datetime,
                                                                             forecast_ts=coverage_file.forecast_dt_start.int_timestamp,
                                                                             issue_dt=coverage_file.forecast_dt_start.datetime,
                                                                             issue_ts=coverage_file.forecast_dt_start.int_timestamp,
                                                                             file_ext=file_ext,
                                                                             pid=pid
                                                                             )
            self.session.add(coverage_file_model)
            self.session.commit()
            self.session.close()
            pass
