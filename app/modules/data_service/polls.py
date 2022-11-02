from typing import Optional, Tuple, Iterable

from sqlalchemy.future import select
from sqlalchemy.orm import relationship, selectinload

from common.meta import SingletonMeta
from connectors.db.postgres import PostgresDataService
from modules.data_service.models import Question


class PollsDataService(PostgresDataService, metaclass=SingletonMeta):
    """Class for data manipulation for polls."""
    def __init__(
            self, host: str, port: int, database: str, user: str, password: str, schema: Optional[str] = None
    ):
        super().__init__(host, port, database, user, password, schema)

    async def get_questions(self, with_relations: Optional[Iterable[relationship]] = None) -> Tuple[Question]:
        """Retrieve data for poll questions.

        :param with_relations: additional relationships to load with questions.
        :type with_relations: Optional[Iterable[relationship]], default None.
        """
        async with self.transaction() as session:
            stmt = select(Question)
            if with_relations:
                stmt = stmt.options(*(selectinload(relation) for relation in with_relations))
            result = await session.execute(stmt)
            questions = result.scalars().all()
            return tuple(questions)

    async def get_choices(self):
        raise NotImplementedError

    async def create_questions(self, questions: Iterable[Question]):
        """Create in database given questions.

        :param questions: questions to create in DB.
        :type questions: Iterable[Question].
        """
        async with self.transaction() as session:
            session.add_all(questions)
