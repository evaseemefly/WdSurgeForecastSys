from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from schema.coverage import CoverageFileUrlSchema, CoverageFileInfoSchema
from models.coverage import GeoCoverageFileModel
from dao.coverage import CoverageDao

app = APIRouter()


@app.get('/one/url/ts', response_model=CoverageFileUrlSchema,
         response_model_include=['remote_url'], summary="获取对应的 tif|nc 文件的远程url", )
def get_coverage_url(issue_ts: int) -> str:
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    url: str = ''
    url = CoverageDao().get_tif_file_url(issue_ts=issue_ts)
    return url


@app.get('/one/info/ts', response_model=CoverageFileInfoSchema,
         response_model_include=['forecast_ts', 'issue_ts', 'task_id', 'relative_path', 'file_name', 'coverage_type'],
         summary="获取对应的 tif|nc 文件的info", )
def get_coverage_url(issue_ts: int) -> Optional[GeoCoverageFileModel]:
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    coverage_info = None
    coverage_info = CoverageDao().get_coveage_file(issue_ts=issue_ts)
    # TODO:[*] 23-06-01 需要将: GeoCoverageFileModel. relative_path+file_name -> remote_url
    return coverage_info
