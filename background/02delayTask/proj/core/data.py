import abc
import pathlib
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from arrow import Arrow
import arrow
import shutil

from pandas import Series

from core.db import DbFactory
from core.files import StationRealDataFile
from conf.settings import DOWNLOAD_OPTIONS
from core.task import TaskFile
from model.station import StationForecastRealDataModel
from util.decorators import decorator_job
from common.enums import JobStepsEnum


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
        now_utc: Arrow = self.now
        stamp_hour = self.now.time().hour
        forecast_dt: Arrow = Arrow(now_utc.date().year, now_utc.date().month, now_utc.date().day, 12, 0)
        # 判断是 00Z 还是 12Z
        # local time : [9,23)
        if stamp_hour >= 1 and stamp_hour < 15:
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
    def download(self, dir_path: str, copy_dir_path: str, key: int) -> StationRealDataFile:
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
        copy_full_path: str = str(pathlib.Path(copy_path) / file_name_str)
        shutil.copy(source_full_path, copy_full_path)
        task_file = TaskFile(copy_path, file_name_str, key)
        self.session.add(task_file)
        self.session.commit()
        self.session.close()
        return StationRealDataFile(copy_path, file_name_str)

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
        @return:
        """
        dict_station_list: dict[str, Series] = station_file.get_station_realdata_list()
        for temp_key in dict_station_list:
            temp_forecast_start_dt: Arrow = station_file.forecast_dt_start
            temp_code: str = temp_key
            surge_list: Series = dict_station_list[temp_code]
            StationForecastRealDataModel.set_split_tab_name(self.get_nearly_forecast_dt())
            for index, temp_surge in enumerate(surge_list):
                temp_dt: Arrow = temp_forecast_start_dt.shift(hours=index)
                temp_station_model: StationForecastRealDataModel = StationForecastRealDataModel(surge=temp_surge,
                                                                                                station_code=temp_code,
                                                                                                forecast_dt=temp_dt.datetime,
                                                                                                forecast_ts=temp_dt.int_timestamp,
                                                                                                issue_dt=self.get_nearly_forecast_dt().datetime,
                                                                                                issue_ts=self.get_nearly_forecast_dt().int_timestamp
                                                                                                )
                self.session.add(temp_station_model)
            self.session.commit()
            self.session.close()
            pass
