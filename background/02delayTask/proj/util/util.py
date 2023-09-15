import uuid
import arrow


def generate_key():
    """
        根据 uuid4 生成数据库唯一主键
    @return:
    """
    return str(uuid.uuid4().hex)[:8]


def get_relative_path(now: arrow.Arrow) -> str:
    """
        根据传入时间获取相对路径
        root/relative_path/file_name
        对于 utc 31d 12h
    @param now:
    @return:
    """
    year_str: str = now.format('YYYY')
    month_str: str = now.format('MM')
    return f'{year_str}/{month_str}'
