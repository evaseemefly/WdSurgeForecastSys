import uuid


def generate_key():
    """
        根据 uuid4 生成数据库唯一主键
    @return:
    """
    return str(uuid.uuid4().hex)[:8]
