import abc
import pathlib
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

from common.default import DEFAULT_FK_STR
from core.db import DbFactory
from core.files import StationRealDataFile, CoverageFile
from conf.settings import DOWNLOAD_OPTIONS
from core.task import TaskFile
from model.station import StationForecastRealDataModel
from model.coverage import GeoCoverageFileModel
from util.decorators import decorator_job
from util.util import get_relative_path
from common.enums import JobStepsEnum, CoverageTypeEnum
from common.comm_dicts import station_code_dicts


class IFileInfo:
    @abc.abstractmethod
    def get_nearly_forecast_dt(self) -> Arrow:
        pass

    @abc.abstractmethod
    def get_file_name(self):
        pass

    @abc.abstractmethod
    def download(self, dir_path: str, copy_dir_path: str) -> StationRealDataFile:
        pass


class StationRealData(IFileInfo):
    def __init__(self, now: Arrow):
        self.now: Arrow = now
        self.session = DbFactory().Session

    def get_nearly_forecast_dt(self) -> Arrow:
        """
            预报文件生成的时间
                预报时间: 00Z  发布时间 09-8=01 15-01
                        12Z          23-8=15 01-15
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
            for index, temp_surge in enumerate(surge_list):
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

    @decorator_job(JobStepsEnum.DOWNLOAD_COVERAGE)
    def download(self, dir_path: str, copy_dir_path: str, key: str) -> Optional[CoverageFile]:
        """
            根据 self.now 进行文件下载
        @param key: task_id
        @param dir_path: 原始路径
        @param copy_dir_path: 存储路径
        @return: StationRealDataFile 海洋站预报潮位文件
        """

        now_year_str: str = self.now.format('YYYY')
        now_month_str: str = self.now.format('MM')
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
        @param dir_path:
        @return:
        """
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
                                                                             file_ext=file_ext,
                                                                             pid=pid
                                                                             )
            self.session.add(coverage_file_model)
            self.session.commit()
