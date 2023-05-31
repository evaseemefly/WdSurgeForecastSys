from datetime import datetime
from arrow import Arrow
import arrow
# 配置文件
from config.tb_config import DB_TABLE_SPLIT_OPTIONS


def get_split_tablename(dt: datetime) -> str:
    """
        根据时间获取对应的分表
    @param dt:
    @return:
    """
    tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
    tab_dt_name: str = arrow.get(dt).format('YYYYMM')
    tab_name: str = f'{tab_base_name}_{tab_dt_name}'
    return tab_name
