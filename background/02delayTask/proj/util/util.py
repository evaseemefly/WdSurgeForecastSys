import uuid
from typing import List

import arrow
import ftplib
import os


def generate_key():
    """
        根据 uuid4 生成数据库唯一主键
    @return:
    """
    return str(uuid.uuid4().hex)[:8]


def get_relative_path(now: arrow.Arrow) -> str:
    """
        根据传入时间获取相对路径
        root/relative_path/file_name
        对于 utc 31d 12h
    @param now:
    @return:
    """
    year_str: str = now.format('YYYY')
    month_str: str = now.format('MM')
    return f'{year_str}/{month_str}'


class FtpFactory:
    """
        + 23-09-20 ftp下载工厂类
    """
    encoding = 'gbk'
    ftp = ftplib.FTP()

    def __init__(self, host: str, port=21):
        self.ftp.connect(host, port)
        self.ftp.encoding = self.encoding

    def login(self, user, passwd):
        self.ftp.login(user, passwd)
        print(self.ftp.welcome)

    def down_load_file(self, local_full_path: str, remote_file_name: str):
        """


        @param local_full_path: 本地存储全路径(含文件名),
        @param remote_file_name: 远端文件名
        @return:
        """
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        self.ftp.retrbinary('RETR ' + remote_file_name, file_handler.write)
        file_handler.close()
        return True

    def down_load_file_bycwd(self, local_full_path: str, remote_path: str, file_name: str) -> bool:
        is_ok: bool = False
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        # 进入到 remote_path 路径下
        self.ftp.cwd(remote_path)
        files_list: List[str] = self.ftp.nlst()
        if file_name in files_list:
            self.ftp.retrbinary('RETR ' + file_name, file_handler.write)
            is_ok = True
        file_handler.close()
        return is_ok

    def down_load_file_tree(self, local_path: str, remote_path: str, cover_path: str = None):
        """
            下载整个目录下的文件
        @param local_path:
        @param remote_path:
        @param cover_path:
        @return:
        """
        print("远程文件夹remoteDir:", remote_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # 进入到 remote_path 路径下
        self.ftp.cwd(remote_path)
        remote_name_list = self.ftp.nlst()
        print("远程文件目录：", remote_name_list)
        # ['EU_high_res_wind_2023082012.nc']
        for file in remote_name_list:
            # 'E:\\05DATA\\06wind\\2022_EU_high'
            Local = os.path.join(local_path, file)
            print("正在下载", self.ftp.nlst(file))
            if file.find(".") == -1:
                if not os.path.exists(Local):
                    os.makedirs(Local)
                self.down_load_file_tree(Local, file)
            else:
                self.down_load_file(Local, file)
        self.ftp.cwd("..")
        return

    def close(self):
        self.ftp.quit()
