from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
#
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
#
from datetime import datetime
from conf.settings import DATABASES


class DbFactory:
    """
        数据库工厂
    """

    def __init__(self, db_mapping: str = 'default', engine_str: str = None, host: str = None, port: str = None,
                 db_name: str = None,
                 user: str = None,
                 pwd: str = None):
        """
            mysql 数据库 构造函数
        :param db_mapping:
        :param engine_str:
        :param host:
        :param port:
        :param db_name:
        :param user:
        :param pwd:
        """
        db_options = DATABASES.get(db_mapping)
        self.engine_str = engine_str if engine_str else db_options.get('ENGINE')
        self.host = host if host else db_options.get('HOST')
        self.port = port if port else db_options.get('POST')
        self.db_name = db_name if db_name else db_options.get('NAME')
        self.user = user if user else db_options.get('USER')
        self.password = pwd if pwd else db_options.get('PASSWORD')
        # TypeError: Invalid argument(s) 'encoding' sent to create_engine(), using configuration MySQLDialect_mysqldb/QueuePool/Engine.  Please check that the keyword arguments are appropriate for this combination of components.
        self.engine = create_engine(
            f"mysql+{self.engine_str}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}",
            pool_pre_ping=True, future=True, echo=True)
        # TODO:[-] 23-03-03 通过 scoped_session 来提供现成安全的全局session
        # 参考: https://juejin.cn/post/6844904164141580302
        self._session_def = scoped_session(sessionmaker(bind=self.engine))

    @property
    def Session(self) -> sessionmaker:
        if self._session_def is None:
            self._session_def = scoped_session(sessionmaker(bind=self.engine))
        return self._session_def()


class MongoDbFactory:
    def __init__(self, db_mapping: str = 'mongo', host: str = None, port: str = None,
                 db_name: str = None,
                 user: str = None,
                 pwd: str = None):
        """
            mongo db 工厂构造函数
        :param db_mapping: 配置文件中 DATABASES 的 mongo 配置项名称
        :param host: 地址
        :param port: 端口
        :param db_name: db 名称
        :param user: 用户名
        :param pwd: 密码
        """
        db_options = DATABASES.get(db_mapping)
        self.host = host if host else db_options.get('HOST')
        self.port = port if port else db_options.get('POST')
        self.db_name = db_name if db_name else db_options.get('NAME')
        self.user = user if user else db_options.get('USER')
        self.password = pwd if pwd else db_options.get('PASSWORD')
        self.connect_str: str = f'{self.db_name}'
        self.connect_engine = None

    def connect(self):
        """
            连接数据库
        :return:
        """
        if self.connect_engine is None:
            self.connect_engine = mongoengine.connect(self.connect_str)
        else:
            return
