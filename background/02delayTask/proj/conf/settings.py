from conf._privacy import DB

db_pwd = DB.get('DB_PWD')
# 数据库的配置，配置借鉴自 django 的 settings 的结构
DATABASES = {
    'default': {
        'ENGINE': 'mysqldb',  # 数据库引擎
        'NAME': 'wd_forecast_db',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': db_pwd,
        'HOST': 'localhost',  # HOST
        # 'HOST': '172.17.0.1',  # 9.79 docker 内部访问 mysql 地址
        # 'HOST': 'host.docker.internal',  # docker 宿主机
        'POST': 3306,  # 端口
        'OPTIONS': {
            "init_command": "SET foreign_key_checks = 0;",
        },
    },

    'mongo': {
        'NAME': 'wave',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': '123456',
        'HOST': 'localhost',  # HOST
        'POST': 27017,  # 端口
    }
}

# 下载配置文件
DOWNLOAD_OPTIONS = {
    # 挂载映射盘路径
    # 'remote_root_path': r'/data/remote',
    'remote_root_path': r'X:',
    # 线上环境
    # 'remote_root_path': r'/home/nmefc/data_remote/71_upload2surge_wd_surge/2023:',
    # 本地下载根目录
    'local_root_path': r'E:\05DATA\01nginx_data\nmefc_download\WD_RESULT'
    # 线上环境
    # 'local_root_path': r'/home/nmefc/data/WD_RESULT'
    # 'local_root_path': r'/data/local'
}

TASK_OPTIONS = {
    'name_prefix': 'TASK_SPIDER_GLOBAL_',
    'interval': 10,  # 单位min
}

DB_TABLE_SPLIT_OPTIONS = {
    'station': {
        'tab_split_name': 'station_realdata_specific'
    }
}
