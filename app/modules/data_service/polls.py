from typing import Optional, Tuple, Iterable, Type

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import relationship, selectinload

from common.meta import SingletonMeta
from connectors.db.postgres import PostgresDataService
from modules.data_service.models import Question, BaseTable


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

    async def create(self, entities: Iterable[Type[BaseTable]]):
        """Create in database given entities.

        :param entities: entities to create in DB.
        :type entities: Iterable[Type[BaseTable]].
        """
        await self._create(entities=entities)

    async def create(self, entities: Iterable[Type[BaseTable]], session: Optional[AsyncSession] = None):
        """Create in database given entities.

        :param entities: entities to create in DB.
        :type entities: Iterable[Type[BaseTable]].
        :param session: session to use for creating entries in database.
        :type session: Optional[AsyncSession], default None.
        """
        if not session:
            async with self.transaction() as session:
                await self._create(entities=entities, session=session)
        else:
            await self._create(entities=entities, session=session)

    async def _create(self, entities: Iterable[Type[BaseTable]], session: AsyncSession):
        """Create in database given entities.

        :param entities: entities to create in DB.
        :type entities: Iterable[Type[BaseTable]].
        :param session: session to use for creating entries in database.
        :type session: AsyncSession.
        """
        session.add_all(entities)
