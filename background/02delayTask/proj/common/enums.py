from enum import Enum, unique


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
    CONVERT_COVERAGE_NC = 1206  # - 读取增水场文件转换至
    CONVERT_COVERAGE_TIF = 1207  # - 读取增水场文件转换至tif


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
    SOURCE_COVERAGE_DOCUMENT = 2101
    CONVERT_COVERAGE_FILE = 2102
    CONVERT_TIF_FILE = 2103
