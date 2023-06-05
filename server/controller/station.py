from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from schema.station_status import StationStatusSchema

from dao.coverage import CoverageDao

app = APIRouter()


@app.get('/status/all', response_model=StationStatusSchema, summary="获取所有站点的状态(最后issue_ts与forecast_ts")
def get_station_list():
    return None
