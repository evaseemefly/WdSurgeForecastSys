# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import arrow
from core.db import DbFactory
from model.base_model import BaseMeta
from model.task import to_migrate
import model.task as tk
from common.comm_dicts import station_code_dicts
from core.case import StationRealDataCase, case_timer_station_forecast_realdata, case_timer_maxsurge_coverage
import model.station as st
import model.coverage as ci


def to_create_db():
    """
        根据orm模型生成数据库表
    @return:
    """
    # engine = DbFactory().engine
    # BaseMeta.metadata.create_all(bind=engine)
    # tk.to_migrate()
    # st.to_migrate()
    ci.to_migrate()
    pass


def test_station_realdata():
    case_timer_station_forecast_realdata()


def test_maxsurge_coverg():
    case_timer_maxsurge_coverage()


# def get_no_exist_staiton_code():


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # to_create_db()
    test_station_realdata()
    # test_maxsurge_coverg()

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
