from typing import List, Optional, Any

from models.task import TaskInfoModel
from models.coverage import GeoCoverageFileModel
from dao.base import BaseDao


class CoverageDao(BaseDao):

    def get_coveage_file(self, forecast_ts: int, issue_ts: int, **kwargs) -> Optional[GeoCoverageFileModel]:
        """
            根据 预报 | 发布 时间戳 获取对应的 nc | tif 文件信息
        @param forecast_ts: 预报时间戳
        @param issue_ts: 发布时间戳
        @param kwargs:
        @return:
        """
        return None
