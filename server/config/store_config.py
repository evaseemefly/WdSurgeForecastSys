import consul


class StoreConfig:
    ip = 'http://localhost:82'

    @classmethod
    def get_ip(cls):
        """
            获取存储动态ip
        @return:
        """
        return cls.ip
