import json
import consul
from typing import Dict, Optional
from config.store_config import StoreConfig


class ConsulConfigClient:
    """
        consul 配置客户端
    """

    def __init__(self, host: str, port: int, ):
        """

        @param host: consul 服务地址
        @param port: consul 端口
        """
        self.host = host
        self.port = port
        self._consul = None
        self._consul = consul.Consul(host=host, port=port)

    def get_consul_kv(self, key: str) -> Optional[Dict]:
        """
            根据key获取对应的配置字典
        @param key: 配置 key
        @return:
        """
        agent_dict = None
        if self._consul is not None:
            _, res = self._consul.kv.get(key)
            val = res.get('Value')
            val_str = val.decode('utf-8')
            val_dict = json.loads(val_str)
            agent_dict = val_dict

        return agent_dict
