# 数据库的配置，配置借鉴自 django 的 settings 的结构
DATABASES = {
    'default': {
        'ENGINE': 'mysqldb',  # 数据库引擎
        'NAME': 'wd_forecast_db',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': '123456',
        # 'HOST': '128.5.9.79',  # HOST
        'HOST': 'host.docker.internal',  # docker 宿主机
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
    'remote_root_path': r'',
    # 本地下载根目录
    'local_root_path': r''
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
