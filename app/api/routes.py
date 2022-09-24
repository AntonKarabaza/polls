from aiohttp import web

from api.views import index, get_questions


def set_up_routes(app: web.Application):
    app.router.add_get('/', index)
    app.router.add_get('/questions', get_questions)
