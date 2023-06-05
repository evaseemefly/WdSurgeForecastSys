from typing import List, Optional, Any

from sqlalchemy import distinct, select
from common.utils import get_remote_url
from config.store_config import StoreConfig
from models.task import TaskInfoModel
from models.coverage import GeoCoverageFileModel
from models.task import TaskInfoModel
from dao.base import BaseDao
from common.enums import CoverageTypeEnum, ForecastProductTypeEnum


class BaseCoverageDao(BaseDao):

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

    def _get_dist_ts_limit(self, limit: int = 10, desc: bool = True) -> List[int]:
        """
            从 tb: geo_coverage_file 中获取不同的 issue_ts，选取 limit 个;
        @param limit: 取出多少个
        @param desc:  是否降序排列（默认降序）
        @return:
        """
        session = self.db.session
        # 方式1
        # query = session.query(distinct(GeoCoverageFileModel.issue_ts))
        # if desc:
        #     # sqlalchemy.exc.CompileError: Can't resolve label reference for ORDER BY / GROUP BY / DISTINCT
        #     # etc. Textual SQL expression '-issue_ts' should be explicitly declared as text('-issue_ts')
        #     query = query.order_by(GeoCoverageFileModel.issue_ts.desc()).all()
        # 方式2: 使用 select 进行复杂查询
        filter_condition = select(GeoCoverageFileModel.issue_ts).group_by(GeoCoverageFileModel.issue_ts)
        if desc:
            filter_condition = filter_condition.order_by(GeoCoverageFileModel.issue_ts.desc()).limit(limit)
        resut = session.execute(filter_condition).fetchall()
        list_dist_ts: List[int] = [ts.issue_ts for ts in resut]
        return list_dist_ts


class CoverageDao(BaseCoverageDao):

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

    def get_dist_ts(self, **kwargs) -> List[int]:
        """
            从 tb: geo_coverage_file 中获取不同的 issue_ts
        @param kwargs:
        @return:
        """
        return self._get_dist_ts_limit()
