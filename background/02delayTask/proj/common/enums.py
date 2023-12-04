from enum import Enum, unique


class RunTypeEnmum(Enum):
    """
        执行 task 类型
    """
    # 延时任务
    DELATY_TASK = 101
    # 即时任务—— 立即执行温带运算任务
    REALTIME_WD = 102
    # 即时任务—— 立即执行风场运算任务
    REALTIME_WIND = 103


class TaskTypeEnum(Enum):
    """
        任务状态枚举
    """
    JOB_STATION = 1101
    JOB_COVERAGE = 1102


class TaskStatusEnum(Enum):
    HANGUP = 1004
    SUCCESS = 1001
    FAIL = 1002
    WAITING = 1003
    RUNNING = 1005


class JobStepsEnum(Enum):
    DOWNLOAD = 1201  # - 下载文件
    DOWNLOAD_STATION = 1202  # - 下载站点文件
    DOWNLOAD_COVERAGE = 1203  # - 下载增水文件
    READFILE_STATION = 1204  # - 读取站点文件
    STORE_DB_STATION = 1205  # - 将站点数据存储至db
    STANDARD_COVERAGE = 1206  # - 标准化coverage文件
    CONVERT_COVERAGE_NC = 1207  # - 读取增水场文件转换至
    CONVERT_COVERAGE_TIF = 1208  # - 读取增水场文件转换至tif
    STORE_DB_COVERAGE = 1209  # 将 coverage 数据存储至 db


class LogLevelEnum(Enum):
    """
        日志等级
    """
    # debug, info, warning, error
    DEBUG = 1301
    INFO = 1302
    WARNING = 1303
    ERROR = 1304


class CoverageTypeEnum(Enum):
    """
        各类栅格文件类型 枚举
    """
    # 原始栅格文件
    SOURCE_COVERAGE_DOCUMENT = 2101
    # 转换后的栅格文件(nc)
    CONVERT_COVERAGE_FILE = 2102
    # 转换后的栅格 tif
    CONVERT_TIF_FILE = 2103
    # NWP 风场原始文件
    NWP_SOURCE_COVERAGE_FILE = 2104
    # NWP 根据经纬度切分后的nc文件
    NWP_SPLIT_COVERAGE_FILE = 2105
    # NWP 按时次提取的tif文件
    NWP_TIF_FILE = 2106
