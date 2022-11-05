from typing import Optional, Tuple, Iterable, Type

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import relationship, selectinload

from common.meta import SingletonMeta
from connectors.db.postgres import PostgresDataService
from modules.data_service.models import BaseTable


class PollsDataService(PostgresDataService, metaclass=SingletonMeta):
    """Class for data manipulation for polls."""
    def __init__(
        self, host: str, port: int, database: str, user: str, password: str, schema: Optional[str] = None
    ):
        super().__init__(host, port, database, user, password, schema)

    async def get(
        self, entity: Type[BaseTable], with_relations: Optional[Iterable[relationship]] = None,
        session: Optional[AsyncSession] = None
    ) -> Tuple[Type[BaseTable]]:
        """Retrieve data for given entity from database.

        :param entity: entity which should be retrieved from database.
        :type entity: Type[BaseTable].
        :param with_relations: additional relationships to load with given entity.
        :type with_relations: Optional[Iterable[relationship]], default None.
        :param session: session to use for retrieving entries from database.
        :type session: Optional[AsyncSession], default None.
        :return: tuple of retrieved entities.
        :rtype: Tuple[Type[BaseTable]].
        """
        if not session:
            async with self.transaction() as session:
                return await self._get(entity=entity, with_relations=with_relations, session=session)
        else:
            return await self._get(entity=entity, with_relations=with_relations, session=session)

    async def _get(
        self, entity: Type[BaseTable], session: AsyncSession, with_relations: Optional[Iterable[relationship]] = None
    ) -> Tuple[Type[BaseTable]]:
        """Retrieve data for given entity from database.

        :param entity: entity which should be retrieved from database.
        :type entity: Type[BaseTable].
        :param session: session to use for retrieving entries from database.
        :type session: AsyncSession.
        :param with_relations: additional relationships to load with given entity.
        :type with_relations: Optional[Iterable[relationship]], default None.
        :return: tuple of retrieved entities.
        :rtype: Tuple[Type[BaseTable]].
        """
        stmt = select(entity)
        if with_relations:
            stmt = stmt.options(*(selectinload(relation) for relation in with_relations))
        result = await session.execute(stmt)
        entities = result.scalars().all()
        return tuple(entities)

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
