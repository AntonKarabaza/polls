# Polls views
from aiohttp import web

from common.utils import to_json
from modules.data_service.polls import PollsDataService


async def index(request):
    return web.Response(text='Hello Aiohttp!')


async def get_questions(request):
    polls_data_service = PollsDataService.get_instance()
    questions = await polls_data_service.get_questions()
    return web.Response(text=to_json(questions))
