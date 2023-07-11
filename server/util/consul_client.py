import json
import abc
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

    def get_action_full_url(self, config_key: str, service_name: str, action_name: str, ) -> Optional[str]:
        """
            + 23-07-11 根据 action 获取对应的 full url
        @param config_key:
        @param service_name:
        @param action_name:
        @return:
        """
        full_url: Optional[str] = None
        action_url: Optional[str] = None
        action_url = self.get_action_url(config_key, service_name, action_name)
        if self.service_instance is not None and len(self.service_instance) > 0:
            instance = self.service_instance[0]
            full_url = f'http://{instance.host}:{instance.port}/{action_url}'
        return full_url
