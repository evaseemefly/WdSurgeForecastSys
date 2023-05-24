from arrow import Arrow

UNLESS_CODE = 'UNLESS'
DEFAULT_CODE = 'DEFAULT'
DEFAULT_NAME = 'DEFAULT'
DEFAULT_EXT = '.nc'
DEFAULT_PATH_TYPE = 'c'  # 默认的台风路径类型 c 为中间路径
UNLESS_RANGE = -999
DEFAULT_SURGE = -9999.99
DEFAULT_FK = -1
NONE_ID = -1
DEFAULT_FK_STR = 'DEFAULT'
DEFAULT_PATH = 'DEFAULT'
UNLESS_INDEX = -1
UNLESS_ID_STR = 'UNLESS'  # + 21-09-06 新加入的未赋值的 id str 类型，为 celery id 使用
DEFAULT_TABLE_NAME = 'DEFAULT_TABLE_NAME'
DEFAULT_YEAR = 1970
DEFAULT_COUNTRY_INDEX = -1
DEFAULT_ENUM = -1
# 默认概率
DEFAULT_PRO = 0.5

# 默认的 arrow 时间 1970-1-1
DEFAULT_ARROW = Arrow(1970, 1, 1)
