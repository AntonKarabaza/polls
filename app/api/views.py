# Polls views
from http import HTTPStatus

from aiohttp import web

from common.utils import to_json
from modules.data_service.models import Question
from modules.data_service.polls import PollsDataService


class Index(web.View):
    """Index web view."""
    async def get(self) -> web.Response:
        return web.Response(text=to_json({'message': 'Hello Aiohttp!'}), status=HTTPStatus.OK)


class QuestionList(web.View):
    """View for manipulating list of questions."""
    async def get(self) -> web.Response:
        polls_data_service = PollsDataService.get_instance()

        try:
            expand_entities = self.request.query['expand'].split(',')
        except KeyError:
            expand_entities = tuple()

        include_relations = tuple(getattr(Question, expand_entity) for expand_entity in expand_entities)
        questions = await polls_data_service.get_questions(with_relations=include_relations)

        return web.Response(
            text=to_json(tuple(question.as_dict(include_relations=include_relations) for question in questions)),
            status=HTTPStatus.OK
        )
