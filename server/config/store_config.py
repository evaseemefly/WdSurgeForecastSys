import consul


class StoreConfig:
    # ip = 'http://192.168.0.104:82'
    ip = 'http://128.5.9.79:82'

    @classmethod
    def get_ip(cls):
        """
            获取存储动态ip
        @return:
        """
        return cls.ip
