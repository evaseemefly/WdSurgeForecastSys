from db.db_factory import DBFactory


class BaseDao:
    """
        + 23-03-09 基础 dao 类
    """

    def __init__(self):
        self.db = DBFactory()
