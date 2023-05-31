# from controller import *
from controller.station_status import app as station_status_app
from controller.station_surge import app as station_surge_app
from controller.region import app as region_app
from controller.station import app as station_app
from controller.station_astronomictide import app as tide_app

urlpatterns = [
    {"ApiRouter": station_status_app, "prefix": "/station/status", "tags": ["海洋站状态"]},
    {"ApiRouter": station_surge_app, "prefix": "/station/surge", "tags": ['海洋站潮位实况']},
    {"ApiRouter": region_app, "prefix": "/region", "tags": ['行政区划']},
    {"ApiRouter": station_app, "prefix": "/station/base", "tags": ['海洋站基础数据']},
    {"ApiRouter": tide_app, "prefix": "/station/tide", "tags": ['海洋站天文潮数据']},
]
