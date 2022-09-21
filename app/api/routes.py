from aiohttp import web

from api.views import index


def set_up_routes(app: web.Application):
    app.router.add_get('/', index)
