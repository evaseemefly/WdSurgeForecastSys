import json
import requests
from typing import List, Optional, Any, Dict

from sqlalchemy import distinct, select, func, and_, text
from sqlalchemy.orm import aliased
from sqlalchemy import select, within_group, distinct
import arrow
from common.utils import get_remote_url
from config.store_config import StoreConfig
from models.station import StationForecastRealDataModel
from schema.station import StationRegionSchema
from schema.station_surge import SurgeRealDataSchema, AstronomicTideSchema, StationTotalSurgeSchema, \
    DistStationTotalSurgeSchema, StationSurgeListSchema, DistStationSurgeListSchema, DistStationTideListSchema
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

    def get_station_max_surge_byissuets(self, issue_ts: int, **kwargs) -> Optional[List[Dict]]:
        """
            + TODO:[*] 23-07-12
              获取所有站点的72小时内的最大增水(issue_ts)
              根据 发布时间获取该发布时间的 max surge集合
        @param issue_ts:
        @param kwargs:
        @return:
        """
        session = self.db.session
        now_arrow: arrow.Arrow = arrow.get(issue_ts)
        # 加入动态修改 tb
        StationForecastRealDataModel.set_split_tab_name(now_arrow)
        #
        #    SELECT MAX(surge),station_code
        #    FROM station_realdata_2023
        #    WHERE issue_ts=1687953600
        #    GROUP BY station_code
        # -----------
        # SELECT station_realdata_2023.station_code, max(station_realdata_2023.surge) AS max_1
        # FROM station_realdata_2023
        # WHERE station_realdata_2023.issue_ts = :issue_ts_1 GROUP BY station_realdata_2023.station_code
        # surge_max_cls = aliased(func.max(StationForecastRealDataModel.surge), name='surge_max')
        stmt = select(StationForecastRealDataModel.station_code, func.max(StationForecastRealDataModel.surge)).where(
            StationForecastRealDataModel.issue_ts == issue_ts).group_by(StationForecastRealDataModel.station_code)
        # stmt = select(StationForecastRealDataModel.station_code, StationForecastRealDataModel.surge).where(
        #     StationForecastRealDataModel.issue_ts == issue_ts).group_by(StationForecastRealDataModel.station_code)
        # stmt = select(StationForecastRealDataModel.station_code, StationForecastRealDataModel.surge).where(
        #     StationForecastRealDataModel.issue_ts == issue_ts)
        # query = session.execute(stmt)
        # # ERROR: sqlalchemy.exc.ResourceClosedError: This result object is closed.
        # res = query.scalars().all()
        # 只有 staiton_code
        # 以下两种写法一致
        # res = session.execute(stmt).scalars().all()
        # res = session.scalars(stmt).all()
        res = session.execute(stmt)
        # surge 保留两位有效数字
        res_list: List[Dict] = [{'code': row[0], 'surge': round(row[1], 2)} for row in res]
        # for temp in res:
        #     print(temp)
        return res_list
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

    def get_stations_hourly_surge_list(self, codes: List[str], issue_ts: int, start_ts: int, end_ts: int, **kwargs):
        """
            獲取指定 codes 的天文潮小時數據集合
        @param codes:
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:
        """
        list_res: List[dict] = []
        for code in codes:
            temp_res = self.get_station_hourly_surge_list(code, issue_ts, start_ts, end_ts)
            list_res.append({'code': code, 'surge_list': temp_res})
        return list_res

    def get_station_hourly_surge_list(self, station_code: str, issue_ts: int, start_ts: int, end_ts: int, **kwargs) -> \
            Optional[List[StationForecastRealDataModel]]:
        """
            获取 站点指定时间范围内的整点数据
            [-] 23-08-14 改善通过 group_by + group_contact 的方式改善了效率
        @param station_code:
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:
        """

        # 获取整点的查询语句
        # """
        #     SELECT *
        #     FROM station_realdata_2023
        #     WHERE issue_ts=1687953600 AND station_code='BHI' AND DATE_FORMAT(forecast_dt,'%i:%s')='00:00'
        #     ORDER BY issue_ts
        #     LIMIT 72
        # """
        session = self.db.session
        limit_count: int = 168
        # 获取整点数据
        # TODO:[-] 23-08-14 暂时去掉 整点的条件，因为增水结果均为整点数据
        # TODO:[*] 23-08-14 此处需要修改为动态获取库表名称
        tab_name: str = 'station_realdata_2023'
        sql_str: str = text(
            f"SELECT * FROM station_realdata_2023 WHERE issue_ts={issue_ts} AND station_code='{station_code}' AND forecast_ts>={start_ts} AND forecast_ts<={end_ts} AND DATE_FORMAT(forecast_dt,'%i:%s')='00:00' ORDER BY issue_ts LIMIT {limit_count}")

        res = session.execute(sql_str)
        res = res.fetchall()
        return res

    def get_dist_stations_hourly_surge_list(self, issue_ts: int, start_ts: int, end_ts: int, **kwargs) -> \
            Optional[List[StationForecastRealDataModel]]:
        """
            获取 站点指定时间范围内的整点数据
            [-] 23-08-14 改善通过 group_by + group_contact 的方式改善了效率
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:[{
            'station_code',
            'surge_list',
            'forecast_ts_list',
            'issue_ts'
        }]
        """

        # 获取整点的查询语句
        # """
        #     SELECT *
        #     FROM station_realdata_2023
        #     WHERE issue_ts=1687953600 AND station_code='BHI' AND DATE_FORMAT(forecast_dt,'%i:%s')='00:00'
        #     ORDER BY issue_ts
        #     LIMIT 72
        # """
        session = self.db.session
        limit_count: int = 72
        # 获取整点数据
        # TODO:[-] 23-08-14 暂时去掉 整点的条件，因为增水结果均为整点数据
        # sql_str: str = text(
        #     f"SELECT * FROM station_realdata_2023 WHERE issue_ts={issue_ts} AND station_code='{station_code}' AND forecast_ts>={start_ts} AND forecast_ts<={end_ts} AND DATE_FORMAT(forecast_dt,'%i:%s')='00:00' ORDER BY issue_ts LIMIT {limit_count}")
        # TODO:[*] 23-08-14 此处需要修改为动态获取库表名称
        tab_name: str = 'station_realdata_2023'
        sql_str: str = text(
            f"SELECT station_code,group_concat(surge) as 'surge_list',group_concat(forecast_ts) as 'forecast_ts_list',issue_ts FROM {tab_name} WHERE issue_ts={issue_ts} AND forecast_ts>={start_ts} AND forecast_ts<={end_ts} GROUP BY station_code")
        res = session.execute(sql_str)
        res = res.fetchall()
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
        stmt = (select(StationForecastRealDataModel.issue_ts).group_by(
            StationForecastRealDataModel.issue_ts).order_by(
            StationForecastRealDataModel.issue_ts).limit(limit_count))
        query = session.scalar(stmt)

        return query


class StationBaseDao(BaseDao):
    """
        站点基础信息 dao (访问台风预报系统)
    """

    def get_dist_station_code(self, **kwargs) -> set:
        """
            获取不同的站点 code set
        @param kwargs:
        @return:
        """
        target_url: str = f'http://128.5.10.21:8000/station/station/all/list'
        res = requests.get(target_url)
        res_content: str = res.content.decode('utf-8')
        # [{'id': 4, 'code': 'SHW', 'name': '汕尾', 'lat': 22.7564, 'lon': 115.3572, 'is_abs': False, 'sort': -1,
        #  'is_in_common_use': True}]
        list_region_dict: List[Dict] = json.loads(res_content)
        list_region: List[StationRegionSchema] = []
        for region_dict in list_region_dict:
            list_region.append(StationRegionSchema.parse_obj(region_dict))
        # 针对code 进行去重操作
        list_codes: List[str] = [station.code for station in list_region]
        return set(list_codes)

    def get_dist_region(self, **kwargs) -> List[str]:
        """
            获取不同的行政区划 code
        @param kwargs:
        @return:
        """

    def get_target_astronomictide(self, code: str, start_ts: int, end_ts: int) -> List[AstronomicTideSchema]:
        """
            获取指定站点的天文潮
            step1: 获取指定站点的 [start,end] 范围内的天文潮集合(间隔1h)
        @param code: 站点
        @param start_ts: 起始时间戳
        @param end_ts: 结束时间戳
        @return:
        """
        target_url: str = f'http://128.5.10.21:8000/station/station/astronomictide/list'
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        start_dt_str: str = f"{start_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        end_dt_str: str = f"{end_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        # 注意时间格式 2023-07-31T16:00:00Z
        # res = requests.get(target_url,
        #                    data={'station_code': code, 'start_dt': start_dt_str, 'end_dt': end_dt_str})
        res = requests.get(target_url,
                           params={'station_code': code, 'start_dt': start_dt_str, 'end_dt': end_dt_str})
        res_content: str = res.content.decode('utf-8')
        # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
        # 天文潮字典集合
        list_tide_dict: List[Dict] = json.loads(res_content)
        # 天文潮 schema 集合
        list_tide: List[AstronomicTideSchema] = []
        for tide_dict in list_tide_dict:
            list_tide.append(AstronomicTideSchema.parse_obj(tide_dict))
        return list_tide

    def get_dist_station_tide_list(self, start_ts: int, end_ts: int) -> List[DistStationTideListSchema]:
        """
            + 23-08-16
            获取所有站点 [start,end] 范围内的 天文潮+时间 集合
        @param start_ts:
        @param end_ts:
        @return:
        """
        target_url: str = f'http://128.5.10.21:8000/station/station/dist/astronomictide/list'
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        start_dt_str: str = f"{start_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        end_dt_str: str = f"{end_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        # 注意时间格式 2023-07-31T16:00:00Z
        # res = requests.get(target_url,
        #                    data={'station_code': code, 'start_dt': start_dt_str, 'end_dt': end_dt_str})
        # TODO:[*] 23-10-24 此接口在高频请求后总会出现无法返回的bug
        res = requests.get(target_url,
                           params={'start_dt': start_dt_str, 'end_dt': end_dt_str})
        res_content: str = res.content.decode('utf-8')
        # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
        # 天文潮字典集合
        list_tide_dict: List[Dict] = json.loads(res_content)
        list_tide: List[DistStationTideListSchema] = []
        for temp in list_tide_dict:
            list_tide.append(DistStationTideListSchema.parse_obj(temp))
        return list_tide


class StationMixInDao(StationBaseDao, StationSurgeDao):
    def get_station_hourly_totalsurge(self, station_code: str, issue_ts: int, start_ts: int, end_ts: int, **kwargs) -> \
            Optional[List[StationTotalSurgeSchema]]:
        """
            获取总潮位集合
            [*] 获取
        @param station_code:
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:
        """
        # TODO:[*] 23-08-13 目前出现的问题是 tide_list 与 surge_list 长度不一致

        # step1: 分別获取憎水 与 天文潮 集合
        # 每小时的增水集合
        # [(81500, 0, 'HZO', 6.79, 'dbdbe2af', 1690819200, 1690804800, datetime.datetime(2023, 7, 31, 16, 0), datetime.datetime(2023, 7, 31, 12, 0))]
        surge_list: Optional[List[StationForecastRealDataModel]] = self.get_station_hourly_surge_list(station_code,
                                                                                                      issue_ts,
                                                                                                      start_ts, end_ts)
        # 每小时的天文潮位
        # [station_code='HZO' forecast_dt='2023-07-31T16:00:00Z' surge=202.0]
        tide_list = self.get_target_astronomictide(station_code, start_ts, end_ts)
        # step2: 按照 station_code 与 forecast_dt 进行拼接
        # 判断 tide_list 与 surge_list 长度是否相同
        total_surge_list: Optional[List[StationTotalSurgeSchema]] = []
        if len(tide_list) == len(surge_list):
            for index, surge in enumerate(surge_list):
                #
                # 0 -(81500,
                # 1 - 0,
                # 2 - 'HZO',
                # 3 - 6.79,
                # 4 - 'dbdbe2af',
                # 5 - 1690819200,
                # 6 - 1690804800,
                # datetime.datetime(2023, 7, 31, 16, 0),
                # datetime.datetime(2023, 7, 31, 12, 0))
                temp_surge = surge
                temp_tide = tide_list[index]
                # 合成的总潮位 shcema
                temp_total_surge: StationTotalSurgeSchema = StationTotalSurgeSchema(station_code=temp_tide.station_code,
                                                                                    forecast_dt=temp_tide.forecast_dt,
                                                                                    forecast_ts=temp_surge[5],
                                                                                    issue_ts=temp_surge[6],
                                                                                    surge=temp_surge[3],
                                                                                    tide=temp_tide.surge,
                                                                                    total_surge=temp_surge[
                                                                                                    3] + temp_tide.surge)
                total_surge_list.append(temp_total_surge)
        return total_surge_list

    def get_dist_stations_surge_list(self, issue_ts: int, start_ts: int, end_ts: int,
                                     **kwargs) -> Optional[List[DistStationSurgeListSchema]]:
        """
            获取所有站点 [start,end] 范围内的 增水集合
        @param issue_ts:
        @param start_ts:
        @param end_ts:
        @param kwargs:
        @return:
        """
        dist_stations_totalsurge_list: Optional[List[DistStationSurgeListSchema]] = []
        # TODO:[-] 23-08-14 此种方法效率较差，已弃用
        # for code in station_codes:
        #     temp_station_totalsurge_schema: Optional[
        #         List[StationTotalSurgeSchema]] = self.get_station_hourly_totalsurge(code, issue_ts, start_ts, end_ts)
        #     temp_dist_station_schema: DistStationTotalSurgeSchema = DistStationTotalSurgeSchema(station_code=code,
        #                                                                                         station_total_schema=temp_station_totalsurge_schema)
        #     dist_stations_totalsurge_list.append(temp_dist_station_schema)
        dist_station_surge_res = self.get_dist_stations_hourly_surge_list(issue_ts, start_ts, end_ts)
        for temp in dist_station_surge_res:
            # ('DGG', '-1.86,...', '1690819200,...', 1690804800)
            temp_code: str = temp[0]
            temp_surge_str_list: List[str] = temp[1].split(',')
            temp_surge_list: List[float] = []
            # TODO:[*] 23-08-21 动态 时间戳 此处会有转换的bug
            # ValueError: could not convert string to float:
            for surge_str in temp_surge_str_list:
                # ValueError: could not convert string to float: '-'
                # 逻辑错了，应该改为 and
                if surge_str != '' and surge_str != '-':
                    temp_surge_list.append(float(surge_str))
            temp_forecast_ts_str_list: List[str] = temp[2].split(',')
            temp_forecast_ts_list: List[int] = []
            for ts_str in temp_forecast_ts_str_list:
                if ts_str != '' or ts_str != '-':
                    temp_forecast_ts_list.append(int(ts_str))
            temp_issue_ts: int = temp[3]
            # print(temp)
            station_surge_list_schema: StationSurgeListSchema = StationSurgeListSchema(
                forecast_ts_list=temp_forecast_ts_list, surge_list=temp_surge_list)
            dist_station_surge_list_schema: DistStationSurgeListSchema = DistStationSurgeListSchema(
                station_code=temp_code, issue_ts=temp_issue_ts, surge_list_schema=station_surge_list_schema)
            dist_stations_totalsurge_list.append(dist_station_surge_list_schema)

        return dist_stations_totalsurge_list
