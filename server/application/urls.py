# from controller import *
from controller.coverage import app as coverage_app
from controller.station import app as station_app

urlpatterns = [
    {"ApiRouter": coverage_app, "prefix": "/coverage", "tags": ["geo coverage"]},
    {"ApiRouter": station_app, "prefix": "/station", "tags": ["station"]},
]
