import pathlib
from typing import List
import xarray as xar
import numpy as np
import pandas as pd
import arrow
from numpy import nan

from config.store_config import STORE_OPTIONS
from dao.base import BaseDao
from models.coverage import GeoCoverageFileModel
from schema.coverage import WindVectorSchema


class BaseVectorDao(BaseDao):
    def __init__(self, coverage_file: GeoCoverageFileModel):
        """

        @param coverage_file:
        """

        # 矢量待读取的栅格文件(nc)
        super(BaseVectorDao, self).__init__()
        self.coverage_file = coverage_file
        pass

    def get_readfile_path(self) -> str:
        root_path: str = STORE_OPTIONS.get('NWP').get('STORE_ROOT_PATH')
        readfile_path: str = f'{root_path}/{self.coverage_file.relative_path}/{self.coverage_file.file_name}'
        return readfile_path


class NWPVectorDao(BaseVectorDao):
    def read_forecast_list(self, lat: float, lon: float) -> List[WindVectorSchema]:
        """
            获取指定经纬度的 风速 | 风向 时序数据
        @param lat:
        @param lon:
        @return:
        """
        # TODO:[*] 23-12-01 重新挂载新的物理硬盘后，读取出错
        # '/data/local_wind_nwp/2023/11/nwp_high_res_wind_2023113012_output.nc'
        readfile_path: str = self.get_readfile_path()
        # TODO:[-] 24-05-27 调试，修改为本地路径
        # readfile_path: str = r'E:\05DATA\99test\WIND\nwp_high_res_wind_2024052612_output.nc'

        field_name = 'ws'

        field_time_name: str = 'time'
        list_vals: List[WindVectorSchema] = []
        if pathlib.Path(readfile_path).is_file():
            ds: xar.Dataset = xar.open_dataset(readfile_path)
            # 通过临近算法获取与当前 lat,lng 最接近的点
            ds = ds.sel(lat=lat, lon=lon, method='nearest')
            # 去掉 'record'
            # ds = ds.isel(record=0)[field_name]
            # target_fields_vals: List[float] = ds.isel(record=0)[field_name].values
            ws_vals: List[float] = ds['ws'].values
            wd_vals: List[float] = ds['wd'].values
            dt64_list: List[np.datetime64] = ds[field_time_name].values
            for index, val in enumerate(ws_vals):
                temp_dt64 = dt64_list[index]
                temp_ts64 = pd.Timestamp(temp_dt64)
                temp_pydt = temp_ts64.to_pydatetime()
                temp_arrow = arrow.get(temp_pydt)
                # TODO:[-] 24-05-27 注意此处的 ws 与 wd 有可能存在为nan的情况，需要加入判断。此处判断不可通过 val== np.nan 或 val is np.nan 的方式进行判断；建议采用 np.isnan(val)
                ws_temp = None if np.isnan(val) else val
                wd_temp = None if np.isnan(wd_vals[index]) else wd_vals[index]
                temp_pydt_utc: arrow.Arrow = temp_arrow.datetime

                list_vals.append(WindVectorSchema(forecast_ts=temp_arrow.int_timestamp, wd=wd_temp, ws=ws_temp))
        return list_vals
