from arrow import Arrow

from util.util import FtpFactory


class StationRealDataMidModel:
    def __init__(self, code: str, organ_code: str, gmt_start: Arrow, gmt_end: Arrow, forecast_source: int,
                 is_forecast: bool = True):
        self.code = code
        self.organ_code = organ_code
        self.gmt_start = gmt_start
        self.gmt_end = gmt_end
        self.source = forecast_source
        self.is_forecast = is_forecast


class FtpClientMidModel:
    def __init__(self, host: str, port: int, user_name: str, pwd: str):
        self.__ftp_client = FtpFactory(host, port)

    @property
    def ftp_client(self):
        return self.__ftp_client
