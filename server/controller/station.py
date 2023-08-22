import arrow
import requests
import json
from typing import List, Type, Any, Optional, Dict
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from schema.station_surge import SurgeRealDataSchema, AstronomicTideSchema, StationTotalSurgeSchema, \
    DistStationTotalSurgeSchema, DistStationSurgeListSchema, DistStationTideListSchema, DistStationAlertLevel
from schema.station import StationRegionSchema, StationRegionSchemaList, StationSurgeJoinRegionSchema
from models.station import StationForecastRealDataModel
from config.consul_config import consul_agent
from dao.station import StationSurgeDao, StationBaseDao, StationMixInDao

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
    """
        获取站点的潮位集合(规定起止范围)——72小时
    @param station_code:
    @param issue_ts:  发布时间戳
    @param start_ts:  起始时间戳
    @param end_ts:  结束时间戳
    @return:  [{
        "station_code": "BHI",
        "forecast_dt": "2023-06-28T12:00:00",
        "issue_dt": "2023-06-28T12:00:00",
        "surge": 0.0,
        "forecast_ts": 1687953600,
        "issue_ts": 1687953600
    },]
    """
    # res_list: Optional[List[StationForecastRealDataModel]] = StationSurgeDao().get_station_surge_list(station_code,
    #                                                                                                   issue_ts,
    #                                                                                                   start_ts, end_ts)
    res_list: Optional[List[StationForecastRealDataModel]] = StationSurgeDao().get_station_hourly_surge_list(
        station_code,
        issue_ts,
        start_ts, end_ts)
    return res_list


@app.get('/inland/list/all', response_model=List[StationRegionSchema],
         response_model_include=['code', 'id', 'name', 'lat', 'lon', 'sort', 'is_in_common_use'],
         summary="获取所有国内的潮位站基础信息集合")
def get_station_all_info():
    """

    @return: {
    id = 4
    code = 'SHW'
    name = '汕尾'
    lat = 22.7564
    lon = 115.3572
    sort = -1
    is_in_common_use = True
    }
    """
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


@app.get('/surge/max/list', response_model=List[Dict],
         summary="获取所有站点的72小时内的最大增水(issue_ts)")
def get_maxsurge_list_byissuets(issue_ts: int):
    """

    @param issue_ts: 发布时间戳
    @return: {
        "code": "AJS",
        "surge": 2.4800000190734863
    },
    """
    res_list = StationSurgeDao().get_station_max_surge_byissuets(issue_ts)
    list_station_baseinfo: List[StationRegionSchema] = get_station_base_info()
    finally_list: List[StationSurgeJoinRegionSchema] = []
    for row in res_list:
        code: str = row['code']
        surge: float = row['surge']
        # name: str = row['name']
        query = list(filter(lambda x: x.code == code, list_station_baseinfo))
        if len(query) > 0:
            row_dict = dict(query[0])
            row_dict['surge'] = surge
            # row_dict['name'] = name
            schema = StationSurgeJoinRegionSchema(**row_dict)
            finally_list.append(schema)

    return finally_list


@app.get('/surge/astronomictide/list', response_model=List[Dict])
def get_astronomictide_list(station_code: str, start_ts: int, end_ts: int):
    """
        + 23-07-20
        获取指定站点的天文潮位
    @param station_code:
    @param start_ts:起始时间戳
    @param end_ts:结束时间戳
    @return:
    """

    # TODO:[*] 23-07-20 此处需要修改为采用服务发现
    # 目前请求地址
    # http://128.5.10.21:8000/station/station/astronomictide/list?station_code=BHI&start_dt=2023-07-19T16:00:00.000Z&end_dt=2023-07-21T16:00:00.000Z
    start_dt_str: str = arrow.get(start_ts).format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z'
    end_dt_str: str = arrow.get(end_ts).format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z'
    # Expected an ISO 8601-like string, but was given '2023-06-28 12:00:00 00:00'. Try passing in a format string to resolve this.
    target_url: str = f'http://128.5.10.21:8000/station/station/astronomictide/list?station_code={station_code}&start_dt={start_dt_str}&end_dt={end_dt_str}'
    res = requests.get(target_url)
    res_content: str = res.content.decode('utf-8')
    list_region: List[Dict] = json.loads(res_content)
    return list_region


@app.get('/alert/one', response_model=List[Dict],
         response_model_include=['station_code', 'tide', 'alert'], summary="获取 station_code 的四色警戒潮位")
def get_station_alert(station_code: str):
    """

    @param station_code:
    @return:
    """
    target_url: str = f'http://128.5.10.21:8000/station/station/alert?station_code={station_code}'
    res = requests.get(target_url)
    res_content: str = res.content.decode('utf-8')
    list_region: List[Dict] = json.loads(res_content)
    return list_region


def get_station_base_info() -> List[StationRegionSchema]:
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


@app.get('/dist/code', response_model=List[Dict],
         response_model_include=['station_code', 'tide', 'alert'], summary="获取 station_code 的四色警戒潮位")
def get_station_alert():
    """
    @return:
    """
    # [{'id': 4, 'code': 'SHW', 'name': '汕尾', 'lat': 22.7564, 'lon': 115.3572, 'is_abs': False, 'sort': -1, 'is_in_common_use': True}]
    list_codes: List[str] = StationBaseDao().get_dist_station_code()
    return list_codes


@app.get('/astornomictide/one', response_model=List[AstronomicTideSchema],
         response_model_include=['station_code', 'forecast_dt', 'surge'], summary="获取 station_code 的四色警戒潮位")
def get_station_astornomictide(station_code: str):
    """
        获取逐时的天文潮
    @param station_code:
    @return:
    """
    start = arrow.get('2023-07-31 16:00:00')
    end = arrow.get('2023-08-02 16:00:00')
    start_ts: int = start.int_timestamp
    end_ts: int = end.int_timestamp
    return StationBaseDao().get_target_astronomictide(station_code, start_ts, end_ts)


@app.get('/totalsurge/one', response_model=List[StationTotalSurgeSchema],
         response_model_include=['station_code', 'forecast_ts', 'issue_ts', 'tide', 'total_surge', 'surge'],
         summary="获取逐时的总潮位( surge:增水 + tide: 天文潮)")
def get_station_totalsurge(station_code: str):
    """
        获取逐时的总潮位( surge:增水 + tide: 天文潮)
    @param station_code:
    @return:[{
        "station_code": "HZO",
        "forecast_ts": 1690819200, 预报时间戳
        "issue_ts": 1690804800,  发布时间戳
        "surge": 6.79, 增水
        "tide": 202.0, 天文潮
        "total_surge": 208.79
    },]
    """
    start = arrow.get('2023-07-31 16:00:00')
    end = arrow.get('2023-08-02 16:00:00')
    issue_ts = 1690804800
    start_ts: int = start.int_timestamp
    end_ts: int = end.int_timestamp
    res: Optional[List[StationTotalSurgeSchema]] = StationMixInDao().get_station_hourly_totalsurge(station_code,
                                                                                                   issue_ts, start_ts,
                                                                                                   end_ts)
    return res


@app.get('/dist/stations/totalsurge', response_model=List[DistStationTotalSurgeSchema],

         summary="获取所有站点的逐时的总潮位( surge:增水 + tide: 天文潮)")
def get_dist_stations_totalsurge(start_ts: int, end_ts: int, issue_ts: int):
    """
        获取所有站点的逐时的总潮位( surge:增水 + tide: 天文潮)
    @return:
    """
    # start = arrow.get('2023-07-31 16:00:00')
    # end = arrow.get('2023-08-02 16:00:00')
    # issue_ts = 1690804800
    # start_ts: int = start.int_timestamp
    # end_ts: int = end.int_timestamp
    # start_ts: int = start
    # end_ts: int = end
    # dist_codes: set = StationBaseDao().get_dist_station_code()
    station_dao = StationMixInDao()
    # 所有站点的增水集合
    list_dist_station_surge: Optional[
        List[DistStationSurgeListSchema]] = station_dao.get_dist_stations_surge_list(
        issue_ts, start_ts, end_ts)
    # station_code='AJS' forecast_ts_list=[1690862400, ...] tide_list=[219.0,...]
    # 所有站点的天文潮集合
    list_dist_station_tide: Optional[List[DistStationTideListSchema]] = station_dao.get_dist_station_tide_list(start_ts,
                                                                                                               end_ts)
    # 所有站点的总潮位集合
    list_dist_station_total: List[DistStationTotalSurgeSchema] = []
    # TODO:[-] 23-08-16 以station_code 将两个集合联结
    for temp_dist_station_surge in list_dist_station_surge:
        filter_res: Optional[List[DistStationTideListSchema]] = list(filter(
            lambda x: temp_dist_station_surge.station_code == x.station_code, list_dist_station_tide))
        if len(filter_res) > 0:
            temp_dist_station_total: DistStationTotalSurgeSchema = DistStationTotalSurgeSchema(
                station_code=temp_dist_station_surge.station_code,
                forecast_ts_list=temp_dist_station_surge.surge_list_schema.forecast_ts_list,
                surge_list=temp_dist_station_surge.surge_list_schema.surge_list, tide_list=filter_res[0].tide_list)
            list_dist_station_total.append(temp_dist_station_total)

        pass
    return list_dist_station_total


@app.get('/dist/stations/alertlevel', response_model=List[DistStationAlertLevel],

         summary="获取所有站点的警戒潮位集合")
def get_dist_stations_alertlevel():
    host: str = 'http://128.5.10.21:8000'
    target_url: str = f'{host}/station/station/dist/alert'
    res = requests.get(target_url, )
    res_content: str = res.content.decode('utf-8')
    # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
    # 天文潮字典集合
    list_tide_dict: List[Dict] = json.loads(res_content)
    list_alert_level: List[DistStationAlertLevel] = []
    for temp in list_tide_dict:
        # {ValidationError}1 validation error for DistStationAlertLevel
        # station
        #   field required (type=value_error.missing)
        list_alert_level.append(DistStationAlertLevel.parse_obj(temp))
    return list_alert_level
