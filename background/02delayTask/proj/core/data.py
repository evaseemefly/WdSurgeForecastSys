import abc
import pathlib

from arrow import Arrow
import shutil

from pandas import Series

from core.db import DbFactory
from core.files import StationRealDataFile
from conf.settings import DOWNLOAD_OPTIONS
from model.station import StationForecastRealDataModel


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

    def download(self, dir_path: str, copy_dir_path: str) -> StationRealDataFile:
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
        return StationRealDataFile(copy_path, file_name_str)

    def __get_nearly_station_surge_list(self):
        """
            根据 self.now 获取临近的时间的文件路径，并读取该文件获取对应的站点预报集合
        :return:
        """
        # file_name: NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_staSurge.txt

    def get_file_name(self):
        forecast_dt: Arrow = self.get_nearly_forecast_dt()
        date_str: str = forecast_dt.format("YYYYMMDDHH")
        file_name: str = f'NMF_TRN_OSTZSS_CSDT_{date_str}_168h_SS_staSurge.txt'
        return file_name

    def to_db(self, station_file: StationRealDataFile):
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
