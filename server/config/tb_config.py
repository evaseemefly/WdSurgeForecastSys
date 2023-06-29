# from config.consul_config import CONSUL_OPTIONS
from config.consul_config import consul_config

# from util.consul import ConsulConfigClient
#
# CONSUL_HOST: str = CONSUL_OPTIONS.get('SERVER').get('HOST')
# CONSUL_PORT: int = CONSUL_OPTIONS.get('SERVER').get('PORT')
# consul_config = ConsulConfigClient(CONSUL_HOST, CONSUL_PORT)

# 温带风暴潮数据库配置
CONSUL_TB_CONFIG = consul_config.get_consul_kv('wd_tb_config')


class TbConfig:
    """
        + 23-06-28
        station tb 分表 配置信息
    """
    station = CONSUL_TB_CONFIG.get('station')

    def get_station_tb_splitname(self) -> str:
        """
            获取 station 的分表 basename
            station -> station_realdata_specific
        @return:
        """
        return self.station.get('station_realdata_specific')


DB_TABLE_SPLIT_OPTIONS = {
    'station': {
        'tab_split_name': 'station_realdata_specific'
    }
}
