from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request


@app.get('/list', response_model=List[StationSchema],
         response_model_include=['station_code', 'station_name', 'lat', 'lon', 'is_in_common_user', 'rid']

    , summary="获取全部潮位站的基础数据(含经纬度信息)", )
def get_all_station():
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    station_list = []
    station_list = StationDao().get_all_station()
    # elif only_country == False and pid != -1:
    #     region_list = RegionDao().get_all_region(pid)
    # pydantic.error_wrappers.ValidationError: 2 validation errors for RegionSchema
    return station_list
