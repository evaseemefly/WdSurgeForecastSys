import json

import consul

# 初始化 Consul 服务
#
cursor = consul.Consul(host='128.5.9.79', port=8500)

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

# 获取服务状态
checks = cursor.agent.checks()
status = checks.get('service:test_consul')
# 根据key val 获取配置
_, res = cursor.kv.get('test_consul')
# 从 res 中获取Value
val = res.get('Value')
val_str = val.decode('utf-8')
val_dict = json.loads(val_str)
print(status)
print(val_dict)
pass
