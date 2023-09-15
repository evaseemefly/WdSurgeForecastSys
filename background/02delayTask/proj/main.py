# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import arrow
import datetime
import time
from apscheduler.schedulers.background import BackgroundScheduler

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


def test_copy_file():
    """
        根据当前时间找到对应文件并copy至指定目录下—— 不需要，放在 station_forecast_realdata 中执行
    @return:
    """
    pass


def test_station_realdata():
    case_timer_station_forecast_realdata()


def test_maxsurge_coverg():
    case_timer_maxsurge_coverage()


def daily_wd_forecast_td() -> None:
    """
        获取温带风暴潮预报产品并执行相关操作
    @return:
    """
    now_start = arrow.now()
    now_format_str: str = now_start.format('YYYY-MM-DDTHH:mm:ss')
    print(f'[*]{now_format_str}:执行每日温带预报产品处理工作流ing!')
    test_station_realdata()
    test_maxsurge_coverg()
    now_end = arrow.now()
    end_format_str: str = now_end.format('YYYY-MM-DDTHH:mm:ss')
    print(f'[-]{end_format_str}:执行每日温带预报产品处理工作流结束')


def timedTask():
    print(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def main():
    # 创建后台执行的 schedulers
    scheduler = BackgroundScheduler(timezone='UTC')
    # scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为 timedTask，触发器选择 interval(间隔性)，间隔时长为 2 秒
    # cron 改为每天 8:50 与 22:50 执行
    # 每天 08:50 | 22:50 执行
    print('[-]启动定时任务触发事件:utc 1,15:10')
    scheduler.add_job(daily_wd_forecast_td, 'cron', hour='1,15', minute='10')
    # scheduler.add_job(timedTask, 'cron', hour='1,15', minute='32')
    # # 启动调度任务
    scheduler.start()
    # daily_wd_forecast_td()

    while True:
        # print(time.time())
        time.sleep(5)


# def get_no_exist_staiton_code():


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # to_create_db()
    # test_station_realdata()
    # test_maxsurge_coverg()
    main()
    print('[-]处理结束')
    pass

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
