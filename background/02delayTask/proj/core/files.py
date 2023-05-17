import abc
import typing
from typing import List
from abc import ABCMeta, abstractmethod, abstractproperty
from arrow import Arrow
import arrow
import pathlib
import pandas as pd
from pandas import Series, DataFrame

from common.comm_dicts import station_code_dicts
from common.default import DEFAULT_ARROW


class IBaseFile(metaclass=ABCMeta):
    def __init__(self, dir_path: str, file_name: str):
        self.dir_path = dir_path
        self.file_name = file_name

    @property
    def name_split(self) -> List[str]:
        """
            三种数据:
                站点:NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_staSurge.txt
                增水场:NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_maxSurge.txt

        :return:
        """
        list_split: List[str] = []
        split_stamp: str = '_'  # 分隔符
        if self.file_name:
            full_name_remove_ext: str = self.file_name[:self.file_name.find('.')]  # 移除了后缀的文件名
            list_split = full_name_remove_ext.split(split_stamp)[1:]  # ['TY2022', '2021010416', 'c0', 'p', '05']
        return list_split

    @property
    def file_name_only(self) -> str:
        """
            不含后缀的文件名
            站点:NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_staSurge.txt
            增水场:NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_maxSurge.txt
        :return:
        """
        return self.file_name.split('.')[0]

    @property
    def full_path(self) -> str:
        file_full_path: str = str(pathlib.Path(self.dir_path) / self.file_name)
        return file_full_path


class StationRealDataFile(IBaseFile):
    """
        海洋站实况 file
        NMF_TRN_OSTZSS_CSDT_2023051612_168h_SS_staSurge.txt
        ----
        0 NMF_
        1 TRN_
        2 OSTZSS_
        3 CSDT_
        4 2023051612_
        5 168h_
        6 SS_
        7 staSurge.txt
    """

    @property
    def forecast_dt_start(self) -> Arrow:
        """
            预报的起始时间(utc)
            self.name_split[4] : 2023051612 -> arrow.Arrow
        :return:
        """
        arrow_start: Arrow = DEFAULT_ARROW
        if len(self.name_split) > 4:
            arrow_start = arrow.get(self.name_split[4], 'YYYYMMDDHH')
            pass
        return arrow_start

    def _check_exist(self):
        """
            判断当前 file 是否存在
        :return:
        """
        file_full_path: str = str(pathlib.Path(self.dir_path) / self.file_name)
        if pathlib.Path(file_full_path).exists():
            return True
        return False

    def get_station_realdata_list(self) -> dict[str, Series]:
        """
            获取 code:series 字典
        :return:
        """
        dict_station = {}
        if self._check_exist():
            with open(self.full_path, 'rb') as f:
                df: DataFrame = pd.read_table(f, encoding='gbk', sep='\s+',
                                              header=0, infer_datetime_format=False)
                columns = df.columns
                for temp_row in columns:
                    temp_code = station_code_dicts.get(temp_row)
                    dict_station[temp_code] = df[temp_code]
            pass
        return dict_station
