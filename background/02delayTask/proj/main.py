# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from core.db import DbFactory
from model.base_model import BaseMeta
from model.task import to_migrate


def to_create_db():
    """
        根据orm模型生成数据库表
    @return:
    """
    # engine = DbFactory().engine
    # BaseMeta.metadata.create_all(bind=engine)
    to_migrate()
    pass


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    to_create_db()

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
