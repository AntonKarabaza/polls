from aiohttp import web

from api.views import Index, QuestionList


def set_up_routes(app: web.Application):
    app.router.add_view('/', Index)
    app.router.add_view('/questions', QuestionList)
