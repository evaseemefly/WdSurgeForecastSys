import json
from random import randint
import requests
from typing import Dict, Optional
import json
import consul
import abc

CONSUL_SETTING = {
    'host': 'localhost',
    'port': 8500
}


class ServiceInstance:

    def __init__(self, service_id: str, host: str, port: int, secure: bool = False, metadata: dict = None,
                 instance_id: str = None):
        self.service_id = service_id
        self.host = host
        self.port = port
        self.secure = secure
        self.metadata = metadata
        self.instance_id = instance_id

    def get_instance_id(self):
        return


class IDiscoveryClient(abc.ABC):

    @abc.abstractmethod
    def get_services(self) -> list:
        pass

    @abc.abstractmethod
    def get_instances(self, service_id: str) -> list:
        pass


class ConsulServiceDiscovery(IDiscoveryClient):
    _consul = None

    def __init__(self, host: str, port: int, token: str = None):
        self.host = host
        self.port = port
        self.token = token
        self._consul = consul.Consul(host, port, token=token)

    def get_services(self) -> list:
        return self._consul.catalog.services()[1].keys()

    def get_instances(self, service_id: str) -> list:
        origin_instances = self._consul.catalog.service(service_id)[1]
        result = []
        for oi in origin_instances:
            result.append(ServiceInstance(
                oi.get('ServiceName'),
                oi.get('ServiceAddress'),
                oi.get('ServicePort'),
                oi.get('ServiceTags'),
                oi.get('ServiceMeta'),
                oi.get('ServiceID'),
            ))
        return result


class ConsulAgentClient:
    """
        consul 代理客户端
        流程:
            step1: 根据服务发现获取服务实例
            step2: 根据 server_key 获取对应服务的urls集合
            step3: 根据 server_action 获取对应的url
            step4: 将 服务实例 host:port/url 进行拼接获取最终的url
    """
    pass

    def __init__(self, consul_host: str, consul_port: int, ):
        self.consul_host = consul_host
        self.consul_port = consul_port
        self.consul_discovery = ConsulServiceDiscovery(self.consul_host, self.consul_port)
        self.service_instance = None
        self.consul_cursor = consul.Consul(self.consul_host, self.consul_port)

    def register(self, service_key: str):
        """
            根据 key 为服务实例进行注册(获取对应的服务实例)
        :param service_key:
        :return:
        """
        self.service_instance = self.consul_discovery.get_instances(service_key)

    def get_config_dict(self, config_key: str) -> Dict:
        """
            根据 config_key 获取对应的配置字典
        :param config_key:
        :return:
        """
        # 根据key val 获取配置
        _, res = self.consul_cursor.kv.get(config_key)
        # 从 res 中获取Value
        val = res.get('Value')
        val_str = val.decode('utf-8')
        val_dict = json.loads(val_str)
        return val_dict

    def get_filter_urls(self, config_key: str, service_name: str, urls_name: str = 'urls') -> Dict:
        dict_config: Dict = self.get_config_dict(config_key)
        dict_urls: Dict = dict_config.get(service_name).get(urls_name)
        return dict_urls

    def get_action_url(self, config_key: str, service_name: str, action_name: str, ) -> Optional[str]:
        dict_urls = self.get_filter_urls(config_key, service_name)
        return dict_urls.get(action_name)
