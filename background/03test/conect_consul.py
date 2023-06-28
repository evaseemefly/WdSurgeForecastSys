import json
from random import randint
import requests
import json
import consul
import abc

CONSUL_SETTING = {
    'host': 'localhost',
    'port': 8500
}


# consul 操作类
# 初始化，指定 consul 主机、端口和 token
class ConsulClient():
    def __init__(self, host=None, port=None, token=None):
        self.host = host  # consul 主机
        self.port = port  # consul 端口
        self.token = token
        self.consul = consul.Consul(host=host, port=port)

    def register(self, name, service_id, address, port, tags, interval, httpcheck):  # 注册服务 注册服务的服务名  端口  以及 健康监测端口
        self.consul.agent.service.register(name, service_id=service_id, address=address, port=port, tags=tags,
                                           interval=interval, httpcheck=httpcheck)

    # 负债均衡获取服务实例
    def getService(self, name):
        # 获取相应服务下的 DataCenter
        url = 'http://' + self.host + ':' + str(self.port) + '/v1/catalog/service/' + name
        dataCenterResp = requests.get(url)
        if dataCenterResp.status_code != 200:
            raise Exception('can not connect to consul ')
        listData = json.loads(dataCenterResp.text)
        # 初始化 DataCenter
        dcset = set()
        for service in listData:
            dcset.add(service.get('Datacenter'))
        # 服务列表初始化
        serviceList = []
        for dc in dcset:
            if self.token:
                url = f'http://{self.host}:{self.port}/v1/health/service/{name}?dc={dc}&token={self.token}'
            else:
                url = f'http://{self.host}:{self.port}/v1/health/service/{name}?dc={dc}&token='
            resp = requests.get(url)
            if resp.status_code != 200:
                raise Exception('can not connect to consul ')
            text = resp.text
            serviceListData = json.loads(text)

            for serv in serviceListData:
                status = serv.get('Checks')[1].get('Status')
                # 选取成功的节点
                if status == 'passing':
                    address = serv.get('Service').get('Address')
                    port = serv.get('Service').get('Port')
                    serviceList.append({'port': port, 'address': address})
        if len(serviceList) == 0:
            raise Exception('no serveice can be used')
        else:
            # 随机获取一个可用的服务实例
            print('当前服务列表：', serviceList)
            service = serviceList[randint(0, len(serviceList) - 1)]
            return service['address'], int(service['port'])

    def get_service_by_health(self, service_name: str):
        index, nodes = self.consul.health.service(service_name, index=None)
        return index, nodes


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


class IServiceRegistry(abc.ABC):

    @abc.abstractmethod
    def register(self, service_instance: ServiceInstance):
        pass

    @abc.abstractmethod
    def deregister(self):
        pass


class ConsulServiceRegistry(IServiceRegistry):
    _consul = None
    _instance_id = None

    def __init__(self, host: str, port: int, token: str = None):
        self.host = host
        self.port = port
        self.token = token
        self._consul = consul.Consul(host, port, token=token)

    def register(self, service_instance: ServiceInstance):
        schema = "http"
        if service_instance.secure:
            schema = "https"
        check = consul.Check.http(f'{schema}:{service_instance.host}:{service_instance.port}/actuator/health', "1s",
                                  "3s", "10s")
        self._consul.agent.service.register(service_instance.service_id,
                                            service_id=service_instance.instance_id,
                                            address=service_instance.host,
                                            port=service_instance.port,
                                            check=check)
        self._instance_id = service_instance.instance_id

    def deregister(self):
        if self._instance_id:
            self._consul.agent.service.deregister(service_id=self._instance_id)
            self._instance_id = None


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


# 请求 consul 实例
class HttpClient():
    # 指定 consul 服务的主机，端口，以及所要请求的应用
    def __init__(self, consulhost, consulport, appname):
        self.consulhost = consulhost
        self.consulport = consulport
        self.appname = appname
        self.consulclient = ConsulClient(host=self.consulhost, port=self.consulport)

    def request(self):
        host, port = self.consulclient.getService(self.appname)
        print('执行当前请求的服务是', host, port)
        scrapyMessage = requests.get(f'http://{host}:{port}/{self.appname}').text
        print('当前服务的相应内容是', scrapyMessage)


def agent_service_register(cursor: consul.Consul):
    """
        通过传入的 cursor 实现服务注册
    :param cursor:
    :return:
    """
    # 注册服务到 Consul
    name = 'api_server'
    cursor.agent.service.register(
        name=name, address='127.0.0.1', port=5000,
        # 心跳检查：间隔：5s，超时：30s，注销：30s
        check=consul.Check().tcp('127.0.0.1', 5000, '5s', '30s', '30s'),
        tags=["v1"],
        service_id=name + "_1"
    )

    cursor.agent.service.register(
        name=name, address='127.0.0.1', port=5001,
        # 心跳检查：间隔：5s，超时：30s，注销：30s
        check=consul.Check().tcp('127.0.0.1', 5001, '5s', '30s', '30s'),
        tags=["v1"],
        service_id=name + "_2"
    )


def agent_serivce_kv(cursor: consul.Consul, kv_key: str) -> dict:
    """
        通过 consul 实现获取 key,val
    :param cursor:
    :param kv_key:
    :return: 返货指定 kv_key 的字典
    """
    # 获取服务状态
    checks = cursor.agent.checks()
    status = checks.get('service:test_consul')
    # 根据key val 获取配置
    _, res = cursor.kv.get(kv_key)
    # 从 res 中获取Value
    val = res.get('Value')
    val_str = val.decode('utf-8')
    val_dict = json.loads(val_str)
    return val_dict


def test_service_register():
    """
        测试通过consul 实现服务注册
    :return:
    """
    # 初始化 Consul 服务
    #
    host: str = CONSUL_SETTING.get('host')
    port: int = CONSUL_SETTING.get('port')
    cursor = consul.Consul(host=host, port=port)
    agent_service_register(cursor)


def test_service_kv():
    cursor = consul.Consul(host='128.5.9.79', port=8500)
    # 测试用例
    # {'driver': 'mysql+mysqldb',
    # 'host': '127.0.0.1',
    # 'port': '3306',
    # 'username': 'root',
    # 'password': '123456',
    # 'database': 'wd_forecast_db',
    # 'charset': 'utf8mb4',
    # 'table_name_prefix': '',
    # 'echo': 0, 'pool_size': 100,
    # 'max_overflow': 100,
    # 'pool_recycle': 60}
    agent_dict = agent_serivce_kv(cursor, 'db_config')
    print(agent_dict)


def test_get_service():
    discovery = ConsulServiceDiscovery("128.5.9.79", 8500)
    # dict_keys(['consul', 'typhoon_forecast_geo_v1', 'typhoon_forecast_station_v1', 'typhoon_forecast_typhoon_v1'])
    services = discovery.get_services()
    # host = {str} '128.5.9.79'
    # instance_id = {str} 'typhoon_forecast_geo_v1_v1'
    # metadata = {dict: 0} {}
    # port = {int} 8092
    # secure = {list: 2} ['master', 'typhoon']
    # service_id = {str} 'typhoon_forecast_geo_v1'
    instances = discovery.get_instances("abc")
    pass


def test_request_service():
    client = HttpClient('128.5.9.79', '8500', 'typhoon_forecast_geo_v1')
    client.request()
    client = HttpClient('128.5.9.79', '8500', 'typhoon_forecast_station_v1')
    client.request()


def test_get_service_health():
    client = HttpClient('128.5.9.79', '8500', 'typhoon_forecast_geo_v1')
    res = client.consulclient.get_service_by_health('typhoon_forecast_geo_v1')
    pass


def main():
    # test_service_kv()
    # test_service_register()
    # test_get_service()
    # test_request_service()
    test_get_service_health()
    pass


if __name__ == '__main__':
    main()
