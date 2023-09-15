from datetime import datetime
from arrow import Arrow

import arrow

# 配置文件
from config.tb_config import DB_TABLE_SPLIT_OPTIONS, TbConfig
from config.store_config import StoreConfig

from models.coverage import GeoCoverageFileModel


def get_split_tablename(dt: datetime) -> str:
    """
        根据时间获取对应的分表
    @param dt:
    @return:
    """
    # tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
    tab_base_name: str = TbConfig().get_station_tb_splitname()
    tab_dt_name: str = arrow.get(dt).format('YYYYMM')
    tab_name: str = f'{tab_base_name}_{tab_dt_name}'
    return tab_name


def get_remote_url(file: GeoCoverageFileModel) -> str:
    """
        根据传入的 file 基础信息获取对应的 remote_url
    @param file:
    @return:
    """
    host: str = StoreConfig.get_ip()
    area: str = 'images/WD_RESULT'
    relative_url: str = f'{file.relative_path}/{file.file_name}'
    full_url: str = f'{host}/{area}/{relative_url}'
    # http: // localhost: 82 / images / nmefc_download /
    return full_url
