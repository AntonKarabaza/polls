from aiohttp import web

from api.contexts import pg_context
from api.routes import set_up_routes


def start_servers(app_config):
    app = web.Application()
    app['config'] = app_config
    app.cleanup_ctx.append(pg_context)
    set_up_routes(app)
    web.run_app(app)
