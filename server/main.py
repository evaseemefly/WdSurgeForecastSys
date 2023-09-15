# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from fastapi import FastAPI
# cors
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import typer

# 项目文件
from application import urls

shell_app = typer.Typer()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:8081",
    "http://0.0.0.0:8081",
    "http://127.0.0.1:8081",
    "http://128.5.9.79:8081",
    "http://128.5.9.79:8087",
]


def init_app():
    app = FastAPI(
        title="wd surge system",
        description="温带风暴潮预报业务系统.本系统通过:fastapi+sqlalchemy+typer实现",
        version="1.0.0"
    )
    # - 23-03-27 加入 cores
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"], )

    for url in urls.urlpatterns:
        # prefix:  '/station/status'
        # tags: ['海洋站状态']
        app.include_router(url["ApiRouter"], prefix=url["prefix"], tags=url["tags"])
    return app


@shell_app.command()
def run():
    """
    启动项目
    """
    uvicorn.run(app='main:init_app', host="0.0.0.0", port=8095)


def main():
    shell_app()


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    main()

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
