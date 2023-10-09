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
    # NWP 风场原始文件
    NWP_SOURCE_COVERAGE_FILE = 2104
    # NWP 根据经纬度切分后的nc文件
    NWP_SPLIT_COVERAGE_FILE = 2105
    # NWP 按时次提取的tif文件
    NWP_TIF_FILE = 2106


class ForecastProductTypeEnum(Enum):
    """
        预报产品种类
    """
    # 预报最大增水场
    FORECAST_MAX_SURGE = 2201
