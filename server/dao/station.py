from typing import List, Optional, Any

from sqlalchemy import distinct, select, func, and_
from sqlalchemy import select, within_group, distinct
import arrow
from common.utils import get_remote_url
from config.store_config import StoreConfig
from models.station import StationForecastRealDataModel
from schema.station_surge import SurgeRealDataSchema
from dao.base import BaseDao
from common.enums import CoverageTypeEnum, ForecastProductTypeEnum


class StationSurgeDao(BaseDao):
    def get_station_last_surge(self, **kwargs) -> Optional[List[SurgeRealDataSchema]]:
        """
            获取各个站点最后时刻的潮位数据及发布时间
        @param kwargs:
        @return:
        """
        session = self.db.session
        now_arrow: arrow.Arrow = arrow.utcnow()
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(now_arrow)
        # StationForecastRealDataModel.__table__.name = 'station_realdata_2023'
        # 执行查询操作
        subquery = session.query(
            StationForecastRealDataModel.station_code,
            func.max(StationForecastRealDataModel.forecast_dt).label('min_forecast')
        ).group_by(StationForecastRealDataModel.station_code).subquery()

        query = session.query(
            StationForecastRealDataModel.station_code.label('station_code'),
            StationForecastRealDataModel.forecast_dt.label('forecast_dt'),
            StationForecastRealDataModel.forecast_ts.label('forecast_ts'),
            StationForecastRealDataModel.issue_ts.label('issue_ts'),
            StationForecastRealDataModel.issue_dt.label('issue_dt'),
            StationForecastRealDataModel.surge.label('surge')
        ).join(
            subquery,
            and_(
                StationForecastRealDataModel.station_code == subquery.c.station_code,
                StationForecastRealDataModel.forecast_dt == subquery.c.min_forecast
            )
        )

        result = query.all()
        schema_list: List[SurgeRealDataSchema] = [
            SurgeRealDataSchema(station_code=temp[0], forecast_dt=temp[1], forecast_ts=temp[2], issue_ts=temp[3],
                                issue_dt=temp[4], surge=temp[5]) for temp in result]
        return schema_list

    def get_station_max_surge_byissuets(self, issue_ts: int, **kwargs) -> Optional[List[SurgeRealDataSchema]]:
        """
            + TODO:[*] 23-07-12
              获取所有站点的72小时内的最大增水(issue_ts)
        @param issue_ts:
        @param kwargs:
        @return:
        """
        pass

    def get_station_surge_list(self, station_code: str, issue_ts: int, start_ts: int, end_ts: int, **kwargs) -> \
            Optional[List[StationForecastRealDataModel]]:
        """
            根据 code 与 发布时间 issue_ts 获取制定时间范围内的 start_ts end_ts 潮位预报数据
        @param station_code:
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:
        """
        session = self.db.session
        issue_arrow: arrow.Arrow = arrow.get(issue_ts)
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(issue_arrow)
        # StationForecastRealDataModel.__table__.name = 'station_realdata_2023'
        query = session.query(StationForecastRealDataModel).filter(StationForecastRealDataModel.issue_ts == issue_ts,
                                                                   StationForecastRealDataModel.station_code == station_code).filter(
            StationForecastRealDataModel.forecast_ts >= start_ts, StationForecastRealDataModel.forecast_ts <= end_ts)
        res = query.all()
        return res

    def get_station_last_issue_ts(self, station_code: str) -> int:
        session = self.db.session
        now_arrow: arrow.Arrow = arrow.utcnow()
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(now_arrow)
        # StationForecastRealDataModel.__table__.name = 'station_realdata_2023'
        query = session.query(func.max(StationForecastRealDataModel.issue_ts)).filter(
            StationForecastRealDataModel.station_code == station_code)
        return query.scalar()

    def get_last_issue_ts(self) -> int:
        """
            + 23-07-12
            获取最近的一个发布时间
        @return:
        """
        session = self.db.session
        now_arrow: arrow.Arrow = arrow.utcnow()
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(now_arrow)
        query = session.query(func.max(StationForecastRealDataModel.issue_ts))
        return query.scalar()

    def get_dist_issue_ts_limit(self, limit_count: int = 10) -> List[int]:
        """
            + 23-07-12
            根据 limit_count 获取 StationForecastRealDataModel 表中最近的 limit_count 个 issue_ts
        @param limit_count:
        @return:
        """
        session = self.db.session
        now_arrow: arrow.Arrow = arrow.utcnow()
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(now_arrow)
        # TODO:[*] 23-07-12 此处未测试
        # 参考2.0特性: https://docs.sqlalchemy.org/en/20/orm/quickstart.html#simple-select
        # https://wiki.masantu.com/sqlalchemy-tutorial/#crud-2
        stmt = (select(StationForecastRealDataModel.issue_ts).group_by(StationForecastRealDataModel.issue_ts).order_by(
            StationForecastRealDataModel.issue_ts).limit(limit_count))
        query = session.scalar(stmt)

        return query
