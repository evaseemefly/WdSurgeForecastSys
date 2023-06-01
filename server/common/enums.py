from enum import Enum, unique


class TaskTypeEnum(Enum):
    """
        任务状态枚举
    """
    HANGUP = 1004
    SUCCESS = 1001
    FAIL = 1002
    WAITING = 1003


class CoverageTypeEnum(Enum):
    """
        栅格种类
    """
    SOURCE_COVERAGE_DOCUMENT = 2101
    CONVERT_COVERAGE_FILE = 2102
    CONVERT_TIF_FILE = 2103


class ForecastProductTypeEnum(Enum):
    """
        预报产品种类
    """
    # 预报最大增水场
    FORECAST_MAX_SURGE = 2201
