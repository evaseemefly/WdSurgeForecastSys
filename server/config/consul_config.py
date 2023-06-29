from util.consul import ConsulConfigClient

# onsul 配置
CONSUL_OPTIONS = {
    'SERVER': {
        'HOST': '128.5.9.79',
        'PORT': 8500
    }
}

CONSUL_HOST: str = CONSUL_OPTIONS.get('SERVER').get('HOST')
CONSUL_PORT: int = CONSUL_OPTIONS.get('SERVER').get('PORT')
consul_config = ConsulConfigClient(CONSUL_HOST, CONSUL_PORT)
