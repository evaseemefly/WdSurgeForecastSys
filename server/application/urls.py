# from controller import *
from controller.coverage import app as coverage_app

urlpatterns = [
    {"ApiRouter": coverage_app, "prefix": "/covrage", "tags": ["geo coverage"]},
]
