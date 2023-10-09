from typing import List, Type, Any, Optional, Dict
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from common.default import DEFAULT_TS
from common.enums import CoverageTypeEnum
from dao.vector import NWPVectorDao
from schema.coverage import CoverageFileUrlSchema, CoverageFileInfoSchema, WindVectorSchema
from models.coverage import GeoCoverageFileModel
from dao.coverage import CoverageDao

app = APIRouter()


@app.get('/one/url/ts', response_model=CoverageFileUrlSchema,
         summary="获取对应的 tif|nc 文件的远程url", )
def get_coverage_url(issue_ts: int, coverage_type: int = CoverageTypeEnum.CONVERT_TIF_FILE.value,
                     forecast_ts: int = DEFAULT_TS) -> Dict[str, str]:
    """
        获取所属当前pid的全部region集合
    @param issue_ts: 发布时间戳
    @param coverage_type:  栅格种类
    @param forecast_ts: 预报时间戳
    @return:
    """
    url: str = ''
    coverage_type_enum: CoverageTypeEnum = CoverageTypeEnum(coverage_type)
    url = CoverageDao().get_tif_file_url(issue_ts=issue_ts, coverage_type=coverage_type_enum, forecast_ts=forecast_ts)
    res = {'remote_url': url}
    return res


@app.get('/one/info/ts', response_model=CoverageFileInfoSchema,
         response_model_include=['forecast_ts', 'issue_ts', 'task_id', 'relative_path', 'file_name', 'coverage_type'],
         summary="获取对应的 tif|nc 文件的info", )
def get_coverage_info(issue_ts: int) -> Optional[GeoCoverageFileModel]:
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:{
    "forecast_ts": 1685620800,
    "issue_ts": 1685620800,
    "task_id": 39268281,
    "relative_path": "2023/06",
    "file_name": "NMF_TRN_OSTZSS_CSDT_2023060112_168h_SS_maxSurge.nc",
    "coverage_type": 2102
}
    """
    coverage_info = None
    coverage_info = CoverageDao().get_coveage_file(issue_ts=issue_ts)
    # TODO:[*] 23-06-01 需要将: GeoCoverageFileModel. relative_path+file_name -> remote_url
    return coverage_info


@app.get('/dist/ts', summary="获取 geo_coverage_file 的不同 issue_ts 并以集合的方式返回,返回最近的10个时间戳", )
def get_dist_ts(limit: int = 10) -> List[int]:
    """
        获取 geo_coverage_file 的不同 issue_ts 并以集合的方式返回
    @param limit:
    @return:
    """
    list_dist_ts: List[int] = CoverageDao().get_dist_ts()
    return list_dist_ts


@app.get('/forecast/point/list', summary="获取 geo_coverage_file 的不同 issue_ts 并以集合的方式返回,返回最近的10个时间戳", )
def get_forecast_list(lat: float, lon: float, issue_ts: int) -> List[WindVectorSchema]:
    """
        step1:根据 issue_ts 获取对应的栅格文件的路径
        step2: 根据指定栅格文件的路径，根据临近算法获取对应点的时序值
    @param lat:
    @param lon:
    @param issue_ts:
    @return:
    """
    # step1: 获取指定nc文件路径
    coverage_file: Optional[GeoCoverageFileModel] = CoverageDao().get_coveage_file(issue_ts=issue_ts,
                                                                                   coverage_type=CoverageTypeEnum.NWP_SPLIT_COVERAGE_FILE)
    # step2: 加载指定nc文件并根据邻近算法获取对应的时序数据
    nwp_forecast_vals: List[WindVectorSchema] = NWPVectorDao(coverage_file).read_forecast_list(lat=lat, lon=lon)
    return nwp_forecast_vals
