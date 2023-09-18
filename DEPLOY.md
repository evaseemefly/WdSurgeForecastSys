#### 系统部署文档

#### 目录:

[TOC]




#### 1- 部署流程图
![001](/docs/imgs/001.png)

#### 2- 容器间的编排

目前本系统主要依赖于两个容器： `wd-forecast-server` 与 `wd-pre`

##### 2-1 wd-pre:

![002](/docs/imgs/002.png)

`wd-pre` 用来定时处理每日两次的温带风暴潮计算任务生成的产品:

 通过/data/remote->下载->/data/local->标准化->to tif -> 提取单站数据 -> to db

###### **目录挂载:**

| Host/volume                                           | Path in container |
| :---------------------------------------------------- | :---------------- |
| /home/nmefc/data/WD_RESULT                            | /data/local       |
| /home/nmefc/proj/wd_surge_background/proj             | /opt/project      |
| /home/nmefc/data_remote/71_upload2surge_wd_surge/2023 | /data/remote      |

###### **docker-compose:**

```docker-compose

version: "3"

services:
  wd-pre:
    build:
      context: .
      dockerfile: ./wd_pro_file
    image: py37:1.19
    container_name: wd-pre
    working_dir: /opt/project
    privileged: true
    command:
    	- /bin/bash
	tty: true
	volumes:
      - /home/nmefc/proj/wd_surge_background/proj:/opt/project
      - /home/nmefc/data_remote/71_upload2surge_wd_surge/2023:/data/remote
      - /home/nmefc/data/WD_RESULT:/data/local

```

*注意:* 

/data/remote 为挂载的共享网络映射盘 在宿主机通过 

`mount -t cifs -o username=xx,password=xx //xx/upload2surge/wd_surge /home/nmefc/data_remote/71_upload2surge_wd_surge`挂在后，在容器类挂载该映射盘。

###### **wd_pro-file:**

```dockerfile
FROM py37:1.19

ENV PYTHONUNBUFFERED 1

# 2- 将 /opt/project 设置为工作目录
WORKDIR /opt/project

# 3- 将本地目录下的文件全部拷贝至容器 /opt/project 中
COPY /home/nmefc/proj/wd_surge_background/proj /opt/project
```



##### 2-2 wd-forecast-server:

温带预报服务主要提供: 基于<u>标量场加载url</u>(通过geotiff的形式供前端加载渲染)、各个单站时序预报数据、以及各类统计信息以及与 **台风集合预报路径系统** 交互获取站点<u>基础信息、天文潮位、四色警戒潮位</u>等信息。

###### 目录挂载

| Host/volume                         | Path in container |
| :---------------------------------- | :---------------- |
| /home/nmefc/proj/wd_forecast_server | /opt/project      |

###### docker-compose:

```docker-compose
version: "3"

services:
  wd-forecast-server:
    build:
      context: .
      dockerfile: ./wd_server_file
    image: py37:1.19
    container_name: wd-forecast-server
    working_dir: /opt/project
    privileged: true
    ports:
      - "8095:8095"
    command:
      - /bin/bash
    tty: true
    volumes:
      - /home/nmefc/proj/wd_forecast_server:/opt/project
```

###### wd_server_file:

```dockerfile
FROM py37:1.19

ENV PYTHONUNBUFFERED 1

# 2- 将 /opt/project 设置为工作目录
WORKDIR /opt/project

# 3- 将本地目录下的文件全部拷贝至容器 /opt/project 中
COPY /home/nmefc/proj/wd_forecast_server /opt/project
```
