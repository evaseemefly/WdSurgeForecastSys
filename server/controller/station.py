import requests
import json
from typing import List, Type, Any, Optional, Dict
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from schema.station_surge import SurgeRealDataSchema
from schema.station import StationRegionSchema, StationRegionSchemaList
from models.station import StationForecastRealDataModel
from config.consul_config import consul_agent
from dao.station import StationSurgeDao

app = APIRouter()


@app.get('/all/last/surge', response_model=List[SurgeRealDataSchema],
         summary="获取所有站点的状态(最后issue_ts与forecast_ts")
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


@app.get('/inland/list/all', response_model=List[StationRegionSchema],
         response_model_include=['code', 'id', 'name', 'lat', 'lon', 'sort', 'is_in_common_use'],
         summary="获取站点的潮位集合(规定起止范围)")
def get_station_all_info():
    list_regions = get_station_base_info()
    # id = 4
    # code = 'SHW'
    # name = '汕尾'
    # lat = 22.7564
    # lon = 115.3572
    # sort = -1
    # is_in_common_use = True
    return list_regions


@app.get('/last/issue_ts/limit', response_model=List[int],
         summary="获取最近的 Limit 个发布时间戳")
def get_last_issuets_limit(limit_count: int):
    """
        获取最近的 Limit 个发布时间戳
    @param limit_count:
    @return:
    """
    res_list: List[int] = StationSurgeDao().get_dist_issue_ts_limit(limit_count)
    return res_list




def get_station_base_info():
    """
        获取全部的站点基础信息
    @return:
    """
    # TODO:[-] 23-07-10 此处修改为通过 consul 动态获取对应的服务地址
    service_key: str = "typhoon_forecast_station_v1"
    config_key: str = 'server_typhoon_forecast'
    consul_agent.register(service_key)
    # target_url: str = 'http://128.5.9.79:8092/station/station/all/list'
    target_url: str = consul_agent.get_action_full_url(config_key, service_key, 'all_stations')

    res = requests.get(target_url)
    res_content: str = res.content.decode('utf-8')
    list_region: List[Dict] = json.loads(res_content)
    # {'id': 4,
    # 'code': 'SHW',
    # 'name': '汕尾',
    # 'lat': 22.7564,
    # 'lon': 115.3572,
    # 'is_abs': False,
    # 'sort': -1,
    # 'is_in_common_use': True
    # }
    list_schema_region: List[StationRegionSchema] = [StationRegionSchema(**item) for item in list_region]
    return list_schema_region
    pass
