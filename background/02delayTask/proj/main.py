# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import pathlib
import sys
import argparse
from typing import Optional, Any

import arrow
import datetime
import time
from apscheduler.schedulers.background import BackgroundScheduler

from common.enums import RunTypeEnmum
from conf._privacy import FTP_LIST
from core.db import DbFactory
from model.base_model import BaseMeta
from model.task import to_migrate
import model.task as tk
from common.comm_dicts import station_code_dicts
from core.case import StationRealDataCase, case_timer_station_forecast_realdata, case_timer_maxsurge_coverage, \
    cast_timer_nwp_wind_coverage
import model.station as st
import model.coverage as ci
from util.util import FtpFactory


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
    # now_start = arrow.Arrow(2023, 10, 26, 8, 0)
    now_start = arrow.now()
    now_format_str: str = now_start.format('YYYY-MM-DDTHH:mm:ss')
    print(f'[*]{now_format_str}:执行每日温带预报产品处理工作流ing!')
    test_station_realdata()
    test_maxsurge_coverg()
    now_end = arrow.now()
    end_format_str: str = now_end.format('YYYY-MM-DDTHH:mm:ss')
    print(f'[-]{end_format_str}:执行每日温带预报产品处理工作流结束')


def daily_nwp_forecast_td() -> None:
    """
        处理每日西北太风场的 case
                local   utc
        更新时间 05:51 ->   22:51
                17:51 ->  9:51
    @return:
    """
    # now_arrow: arrow.Arrow = arrow.Arrow(2023, 9, 26, 0, 0)
    # TODO:[-] 23-09-27 取每次触发时的整点时间
    now_hourly_arrow = arrow.utcnow().floor('hour')
    cast_timer_nwp_wind_coverage(now_hourly_arrow)


def immediately_forecast_td() -> None:
    """
        立即执行 下载处理风场 | 下载处理wd预报产品流程
    @return:
    """
    # 执行下载西北太ecremix风场流程
    daily_nwp_forecast_td()
    # 执行下载温带预报产品流程
    daily_wd_forecast_td()
    pass


def timedTask():
    print(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def delayed_task():
    """
        执行 延时任务
        通过 Scheduler 实现定时任务执行
    @return:
    """
    # 创建后台执行的 schedulers
    scheduler = BackgroundScheduler(timezone='UTC')
    # scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为 timedTask，触发器选择 interval(间隔性)，间隔时长为 2 秒
    # cron 改为每天 8:50 与 22:50 执行
    # 每天 08:50 | 22:50 执行
    print('[-]启动定时任务触发事件:utc 1,15:10')
    # 每日定时处理温带预报产品
    # 首次计算
    scheduler.add_job(daily_wd_forecast_td, 'cron', hour='0,15', minute='10')
    # 补算
    scheduler.add_job(daily_wd_forecast_td, 'cron', hour='1', minute='10')
    # TODO:[*] 23-09-27
    # 每日定时处理西北太风场
    # 更新时间
    # 05: 51 ->   22: 51
    # 17: 51 ->  9: 51
    scheduler.add_job(daily_nwp_forecast_td, 'cron', hour='9,22', minute='59')
    # scheduler.add_job(daily_nwp_forecast_td, 'cron', hour='9,22', minute='55')

    # scheduler.add_job(timedTask, 'cron', hour='1,15', minute='32')
    # # 启动调度任务
    scheduler.start()


switch_dict = {
    # 定时任务执行
    RunTypeEnmum.DELATY_TASK: delayed_task,
    # 补算立即执行任务： 温带
    RunTypeEnmum.REALTIME_WD: daily_wd_forecast_td,
    # 补算立即执行任务： 风场
    RunTypeEnmum.REALTIME_WIND: daily_nwp_forecast_td
}


def main(run_type: RunTypeEnmum = RunTypeEnmum.DELATY_TASK):
    do_func: Optional[Any] = switch_dict.get(run_type)
    if do_func is None:
        raise Exception('传入run_type参数错误')
    else:
        do_func()

    while True:
        # print(time.time())
        time.sleep(5)


# def get_no_exist_staiton_code():


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # to_create_db()
    # test_station_realdata()
    # test_maxsurge_coverg()
    # 获取运行时传入的参数
    parser = argparse.ArgumentParser(description='传入运行时参数')
    parser.add_argument('--run_type', help='运行类型', default=RunTypeEnmum.DELATY_TASK.value, type=int)
    args = parser.parse_args()
    # args_dict = vars(args)
    # # 取出 run_type 参数
    # run_type_val: str = args_dict.get('run_type')
    # 获取 run_type 枚举 key(int)
    run_type_key: int = args.run_type
    #
    # run_type: RunTypeEnmum = RunTypeEnmum(int(run_type_val))
    run_type: RunTypeEnmum = RunTypeEnmum(run_type_key)
    main(run_type)
    print('[-]处理结束')
    pass

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
