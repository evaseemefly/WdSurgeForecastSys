from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from schema.coverage import CoverageFileUrlSchema, CoverageFileInfoSchema
from models.coverage import GeoCoverageFileModel
from dao.coverage import CoverageDao

app = APIRouter()


@app.get('/one/url/ts', response_model=CoverageFileUrlSchema,
         summary="获取对应的 tif|nc 文件的远程url", )
def get_coverage_url(issue_ts: int) -> str:
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    url: str = ''
    url = CoverageDao().get_tif_file_url(issue_ts=issue_ts)
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


@app.get('/dist/ts', summary="获取 geo_coverage_file 的不同 issue_ts 并以集合的方式返回", )
def get_dist_ts(limit: int = 10) -> List[int]:
    """
        获取 geo_coverage_file 的不同 issue_ts 并以集合的方式返回
    @param limit:
    @return:
    """
    list_dist_ts: List[int] = CoverageDao().get_dist_ts()
    return list_dist_ts
