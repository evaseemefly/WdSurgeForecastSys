from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from schema.station_surge import SurgeRealDataSchema
from models.station import StationForecastRealDataModel

from dao.station import StationSurgeDao

app = APIRouter()


@app.get('/all/last/surge', response_model=List[SurgeRealDataSchema], summary="获取所有站点的状态(最后issue_ts与forecast_ts")
def get_station_all_last_surge():
    schema_list: Optional[List[SurgeRealDataSchema]] = StationSurgeDao().get_station_last_surge()
    return schema_list


@app.get('/one/last/issue', summary="获取指定站点的最后issue_ts")
def get_target_station_last_issue_ts(station_code: str) -> Optional[int]:
    res = StationSurgeDao().get_station_last_issue_ts(station_code)
    return res


@app.get('/surge/list', response_model=List[SurgeRealDataSchema],
         response_model_include=['station_code', 'forecast_dt', 'forecast_ts', 'issue_dt', 'issue_ts', 'surge'],
         summary="获取站点的潮位集合(规定起止范围)")
def get_station_surge_list(station_code: str, issue_ts: int, start_ts: int, end_ts: int):
    res_list: Optional[List[StationForecastRealDataModel]] = StationSurgeDao().get_station_surge_list(station_code,
                                                                                                      issue_ts,
                                                                                                      start_ts, end_ts)
    return res_list
