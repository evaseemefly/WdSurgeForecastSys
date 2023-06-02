from typing import List, Optional, Any

from common.utils import get_remote_url
from config.store_config import StoreConfig
from models.task import TaskInfoModel
from models.coverage import GeoCoverageFileModel
from models.task import TaskInfoModel
from dao.base import BaseDao
from common.enums import CoverageTypeEnum, ForecastProductTypeEnum


class CoverageDao(BaseDao):

    def get_coveage_file(self, issue_ts: int, **kwargs) -> Optional[GeoCoverageFileModel]:
        """
            根据 预报 | 发布 时间戳 获取对应的 nc | tif 文件信息
        @param forecast_ts: 预报时间戳 ! 不再使用，由于 对于 coverage 而言 发布时间即是预报时间
        @param issue_ts: 发布时间戳
        @param kwargs:
        @return:
        """
        session = self.db.session
        query = session.query(GeoCoverageFileModel).filter(issue_ts == issue_ts)
        if kwargs.get('coverage_type') is not None:
            coverage_type: CoverageTypeEnum = kwargs.get('coverage_type')
            query = query.filter(coverage_type == coverage_type.value)
        return query.first()

    def get_nc_file_url(self, issue_ts: int, forecast_product_type: ForecastProductTypeEnum) -> str:
        """
            根据 发布时间 + 预报产品种类 获取 nc 文件 url
        @param issue_ts:
        @param forecast_product_type:
        @return:
        """
        return ''

    def get_tif_file_url(self, issue_ts: int) -> str:
        """
            根据 发布时间 + 预报产品种类 获取 tif 文件 url
        @param issue_ts:
        @param forecast_product_type:
        @return:
        """
        file_info: GeoCoverageFileModel = self.get_coveage_file(issue_ts,
                                                                coverage_type=CoverageTypeEnum.CONVERT_TIF_FILE)
        full_url: str = get_remote_url(file_info)

        return full_url

    def get_file_url(self, **kwargs) -> str:
        """
            根据条件获取 file remote url
        @param kwargs:
        @return:
        """
        return ''
