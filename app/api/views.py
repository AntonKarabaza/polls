# Polls views
from http import HTTPStatus

from aiohttp import web

from common.utils import to_json
from modules.data_service.polls import PollsDataService


class Index(web.View):
    """Index web view."""
    async def get(self):
        return web.Response(text=to_json({'message': 'Hello Aiohttp!'}), status=HTTPStatus.OK)


class QuestionList(web.View):
    """View for manipulating list of questions."""
    async def get(self):
        polls_data_service = PollsDataService.get_instance()
        questions = await polls_data_service.get_questions()
        return web.Response(
            text=to_json(tuple(question.as_dict() for question in questions)),
            status=HTTPStatus.OK
        )
