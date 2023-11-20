# coding=utf-8
# from consulate import Consul
import logging
from typing import Optional, Dict

import consul
from random import randint
import requests
import json

from config import consul_config


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


class ConsulClient:
    """
        # TODO:[*] 23-11-01 参考文章
        # https://gitee.com/aichinai/consul_flask/blob/master/ConsulFlask/consulclient.py
    """

    def __init__(self, host=None, port=None, token=None):
        """
            consul 操作类
        :param host:
        :param port:
        :param token:
        """

        self.host = host  # consul 主机
        self.port = port  # consul 端口
        self.token = token
        self.consul = consul.Consul(host=host, port=port)

    def register(self, name, service_id, address, port, tags, interval, httpcheck):
        # 注册服务 注册服务的服务名  端口  以及 健康监测端口
        self.consul.agent.service.register(name, service_id=service_id, address=address, port=port, tags=tags,
                                           interval=interval, check=httpcheck)
        pass

    def get_service(self, name) -> str:
        """
            负载均衡获取服务实例
        :param name:
        :return:
        """
        # client: 'http://127.5.9.79:8500/v1/catalog/service/student-service'

        # 尝试通过
        # self.consul.agent.services()
        # {ConnectionError}HTTPConnectionPool(host='127.5.9.79', port=8500): Max retries exceeded with url: /v1/agent/services (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x000001A9CD73BE48>: Failed to establish a new connection: [WinError 10061] 由于目标计算机积极拒绝，无法连接。'))
        url_back = 'http://' + self.host + ':' + str(self.port) + '/v1/catalog/service/' + name  # 获取 相应服务下的DataCenter

        url: str = f'http://{self.host}:{self.port}/v1/catalog/service/{name}'
        requests.adapters.DEFAULT_RETRIES = 5  # 增加重连次数

        req = requests.session()
        req.keep_alive = False  # 关闭多余连接

        dataCenterResp = req.get(url)
        if dataCenterResp.status_code != 200:
            raise Exception('连接 consul 错误')
        listData = json.loads(dataCenterResp.text)
        dcset = set()  # DataCenter 集合 初始化
        for service in listData:
            dcset.add(service.get('Datacenter'))
        serviceList = []  # 服务列表 初始化
        for dc in dcset:
            target_url: str = f'http://{self.host}:{self.port}/v1/health/service/{name}?dc={dc}&token='
            if self.token:
                # url = 'http://' + self.host + ':' + self.port + '/v1/health/service/' + name + '?dc=' + dc + '&token=' + self.token
                target_url = f'{target_url}{self.token}'
            else:
                # TypeError: can only concatenate str (not "int") to str
                # url = 'http://' + self.host + ':' + self.port + '/v1/health/service/' + name + '?dc=' + dc + '&token='
                pass
            resp = requests.get(target_url)
            if resp.status_code != 200:
                raise Exception('连接 consul 错误 ')
            text = resp.text
            serviceListData = json.loads(text)

            for serv in serviceListData:
                status = serv.get('Checks')[1].get('Status')
                if status == 'passing':  # 选取成功的节点
                    address = serv.get('Service').get('Address')
                    port = serv.get('Service').get('Port')
                    serviceList.append({'port': port, 'address': address})

        # print("成功节点服务列表：", serviceList)
        if len(serviceList) == 0:
            raise Exception('没有服务可用')
            # print("没有服务可用")
        else:
            service = serviceList[randint(0, len(serviceList) - 1)]  # 随机获取一个可用的服务实例
            # print("返回随机选取的节点", service['address'], int(service['port']))
            # return service['address'],int(service['port'])
            service_url: str = f'http://{service["address"]}:{service["port"]}'
            return service_url


class ConsulRegisterServer:
    """
        与 start_up 事件绑定并触发实现注册服务
    """

    # 新建服务时，需要指定consul服务的 主机，端口，所启动的 服务的 主机 端口 以及 restful http 服务 类
    def __init__(self, service_host, service_port, consul_host, consul_port, app_name):
        """

        :param service_host: 127.0.0.1  当前服务的实际ip地址
        :param service_port: 8000
        :param consul_host: 128.5.9.79   consul 地址
        :param consul_port: 8500
        :param app_name: 注册的服务名称
        """
        self.service_port = service_port
        self.service_host = service_host
        # self.app = appClass(host=host, port=port)
        # self.appname = self.app.appname
        self.app_name = app_name
        self.consul_host = consul_host
        self.consul_port = consul_port

    def register(self):
        """
            基于 self.service_host:self.service_port 注册至 self.consul_host:self.consul_port

        :return:
        """
        client = ConsulClient(host=self.consul_host, port=self.consul_port)
        service_id = self.app_name + self.service_host + ':' + str(self.service_port)
        # 暂时不用
        httpcheck = 'http://' + self.service_host + ':' + str(self.service_port) + '/check'
        # TODO:[*] 23-11-02 注意此处的 check 有错误，无法实现心跳检测
        # 注意心跳检测 check 的 host 应为 consul 的地址，改为consul的地址
        # consul.base.BadRequest: 400 Invalid service address
        client.register(self.app_name, service_id=service_id, address=self.service_host, port=self.service_port,
                        tags=['master'],
                        interval='30s',
                        httpcheck=consul.Check().tcp(self.consul_host, self.consul_port, '5s', '30s', '30s'))  # 注册服务
        pass
        # self.app.run()  # 启动服务
        # uvicorn.run(app=app, host=self.host, port=self.port)


class ConsulExtractClient:
    """
        consul 获取(客户端)类
        # TODO:[*] 23-11-06 参考文章: https://www.jianshu.com/p/717a90211b8c
    """

    def __init__(self, project_name):
        """初始化，连接consul服务器"""
        # self.sr = SR(project)
        self.project_name = project_name
        self.consul = ConsulClient(host=consul_config.CONSUL_HOST,
                                   port=consul_config.CONSUL_PORT)
        self.client = requests.session()
        self.headers = {'Content-Type': 'application/json', 'Connection': 'keep-alive',
                        'buc-auth-token': 'default.buc-auth-token'}

    def get(self, uri, decode='utf-8', is_break: bool = False, **kwargs) -> Optional[str]:
        """
            获取指定服务地址并返回结果
        :param uri: 请求的服务的 /area/controller
        :param decode: 默认: utf-8 用来将 response.content 进行解码
        :param is_break: 默认: false 若关闭则不会进行多次请求
        :param kwargs: 若提交需要传入参数，则通过 params:type=dict
        :return:
        """
        try_count = 0
        # 加入了多次请求的逻辑
        #
        address = self.consul.get_service(self.project_name)
        # 拼接对应的服务请求地址
        # address : http://xxx:xx/ 是对应的注册服务的地址
        url = '{0}{1}'.format(address, uri)
        if kwargs.get('params') is not None:
            response = self.client.get(
                url=url,
                params=kwargs.get('params'),
                headers=self.headers)
        else:
            response = self.client.get(
                url=url,
                headers=self.headers)
        if response.status_code == 200:
            return response.content.decode(decode)
            # return response.content
        else:
            logging.warning('{0}: status_code is {1}'.format(url, response.status_code))
        # TODO:[*] 23-11-20 此处取消多次复联的逻辑，会造成延迟较长的连接会多次请求
        # while try_count < consul_config.CONSUL_MAX_TRY_COUNT:
        #     address = self.consul.get_service(self.project_name)
        #     # 拼接对应的服务请求地址
        #     # address : http://xxx:xx/ 是对应的注册服务的地址
        #     url = '{0}{1}'.format(address, uri)
        #     if kwargs.get('params') is not None:
        #         response = self.client.get(
        #             url=url,
        #             params=kwargs.get('params'),
        #             headers=self.headers)
        #     else:
        #         response = self.client.get(
        #             url=url,
        #             headers=self.headers)
        #     # if response.status_code is 200:
        #     #     self.aiops_consul.update_weight(address, success=True)
        #     #     return response.content
        #     # self.aiops_consul.update_weight(address, success=False)
        #     if response.status_code == 200:
        #         return response.content.decode(decode)
        #         # return response.content
        #     else:
        #         if is_break:
        #             break
        #         try_count += 1
        #         logging.warning('{0}: status_code is {1}'.format(url, response.status_code))
        raise Exception('service service is error')

    def post(self, uri, data):
        try_count = 0
        while try_count < 5:
            address = self.consul.get_service(self.project_name)
            url = '{0}{1}'.format(address, uri)
            response = self.client.post(
                url=url,
                headers=self.headers,
                data=data)
            # if response.status_code is 200:
            #     self.aiops_consul.update_weight(address, success=True)
            # return response.content
            # self.aiops_consul.update_weight(address, success=False)
            try_count += 1
            logging.warning('{0}: status_code is {1}'.format(url, response.status_code))
        raise Exception('service service is error')

    def get_task(self, task_id, user):
        return self.get('/task/get/{0}?user={1}'.format(task_id, user))

    def get_and_run_task(self, task, user):
        return self.get('/task/getAndRun/{0}?user={1}'.format(task, user))
