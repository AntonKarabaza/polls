# Polls views
from http import HTTPStatus

from aiohttp import web

from common.utils import to_json
from modules.data_service.models import Question, Choice
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

    async def post(self) -> web.Response:
        polls_data_service = PollsDataService.get_instance()
        wrapped_questions = []
        posted_questions = await self.request.json()

        for question in posted_questions:
            wrapped_question_choices = []
            for choice in question.pop('choices', []):
                wrapped_question_choices.append(Choice(**choice))
            wrapped_questions.append(Question(**question, choices=wrapped_question_choices))

        await polls_data_service.create_questions(wrapped_questions)

        return web.Response(
            text=to_json(tuple(question.as_dict() for question in wrapped_questions)),
            status=HTTPStatus.OK
        )
