from aiohttp import web

from api.routes import set_up_routes


def start_servers(app_config):
    app = web.Application()
    set_up_routes(app)
    app['config'] = app_config
    web.run_app(app)
