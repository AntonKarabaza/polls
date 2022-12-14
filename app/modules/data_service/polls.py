from functools import partial
from typing import Optional, Tuple, Iterable, Type, MutableMapping, Any

from sqlalchemy import Column, update, delete
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
        self,
        entity: Type[BaseTable],
        conditions: MutableMapping[Column, Any] = None,
        with_relations: Optional[Iterable[relationship]] = None,
        session: Optional[AsyncSession] = None,
    ) -> Tuple[BaseTable]:
        """Retrieve data for given entity from database.

        :param entity: entity which should be retrieved from database.
        :type entity: Type[BaseTable].
        :param conditions: mapping of columns to values which should be used as conditions while retrieving.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param with_relations: additional relationships to load with given entity.
        :type with_relations: Optional[Iterable[relationship]], default None.
        :param session: session to use for retrieving entries from database.
        :type session: Optional[AsyncSession], default None.
        :return: tuple of retrieved entities.
        :rtype: Tuple[Type[BaseTable]].
        """
        get = partial(
            self._get,
            entity=entity,
            conditions=conditions,
            with_relations=with_relations,
        )
        if not session:
            async with self.transaction() as session:
                return await get(session=session)
        else:
            return await get(session=session)

    async def _get(
        self,
        entity: Type[BaseTable],
        session: AsyncSession,
        conditions: MutableMapping[Column, Any] = None,
        with_relations: Optional[Iterable[relationship]] = None,
    ) -> Tuple[BaseTable]:
        """Retrieve data for given entity from database.

        :param entity: entity which should be retrieved from database.
        :type entity: Type[BaseTable].
        :param session: session to use for retrieving entries from database.
        :type session: AsyncSession.
        :param conditions: mapping of columns to values which should be used as conditions while retrieving.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param with_relations: additional relationships to load with given entity.
        :type with_relations: Optional[Iterable[relationship]], default None.
        :return: tuple of retrieved entities.
        :rtype: Tuple[Type[BaseTable]].
        """
        stmt = select(entity)
        for column, value in (conditions or {}).items():
            stmt = stmt.where(column == value)
        if with_relations:
            stmt = stmt.options(*(selectinload(relation) for relation in with_relations))
        result = await session.execute(stmt)
        entities = result.scalars().all()
        return tuple(entities)

    async def create(self, entities: Iterable[BaseTable], session: Optional[AsyncSession] = None):
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

    async def _create(self, entities: Iterable[BaseTable], session: AsyncSession):
        """Create in database given entities.

        :param entities: entities to create in DB.
        :type entities: Iterable[Type[BaseTable]].
        :param session: session to use for creating entries in database.
        :type session: AsyncSession.
        """
        session.add_all(entities)

    async def update(
        self,
        entity: Type[BaseTable],
        set_values: MutableMapping[Column, Any],
        conditions: Optional[MutableMapping[Column, Any]] = None,
        returning: bool = False,
        session: Optional[AsyncSession] = None,
    ) -> Optional[Iterable[BaseTable]]:
        """Update in database given entity.

        :param entity: entity which should be updated in database.
        :type entity: Type[BaseTable].
        :param set_values: mapping of columns to values which should be set during update.
        :type set_values: MutableMapping[Column, Any].
        :param conditions: mapping of columns to values which should be used as conditions during update.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param returning: condition if updated entities should be returned after update.
        :type returning: bool, default False.
        :param session: session to use for updating entries in database.
        :type session: Optional[AsyncSession], default None.
        :return: updated entities or None.
        :rtype: Optional[Iterable[BaseTable]].
        """
        update = partial(
            self._update,
            entity=entity,
            set_values=set_values,
            conditions=conditions,
            returning=returning,
        )
        if not session:
            async with self.transaction() as session:
                return await update(session=session)
        else:
            return await update(session=session)

    async def _update(
        self,
        entity: Type[BaseTable],
        set_values: MutableMapping[Column, Any],
        session: AsyncSession,
        conditions: MutableMapping[Column, Any] = None,
        returning: bool = False,
    ) -> Optional[Iterable[BaseTable]]:
        """Update in database given entity.

        :param entity: entity which should be updated in database.
        :type entity: Type[BaseTable].
        :param set_values: mapping of columns to values which should be set during update.
        :type set_values: MutableMapping[Column, Any].
        :param session: session to use for updating entries in database.
        :type session: AsyncSession.
        :param conditions: mapping of columns to values which should be used as conditions during update.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param returning: condition if updated entities should be returned after update.
        :type returning: bool, default False.
        :return: updated entities or None.
        :rtype: Optional[Iterable[BaseTable]].
        """
        stmt = update(entity).values(**{column.name: value for column, value in set_values.items()})
        for column, value in (conditions or {}).items():
            stmt = stmt.where(column == value)
        if returning:
            stmt = stmt.returning(entity)

        result = await session.execute(stmt)

        if returning:
            entities = tuple(entity(**record) for record in result.fetchall())
            return entities

    async def delete(
        self,
        entity: Type[BaseTable],
        conditions: Optional[MutableMapping[Column, Any]] = None,
        returning: bool = False,
        session: Optional[AsyncSession] = None,
    ) -> Optional[Iterable[BaseTable]]:
        """Delete from database given entity.

        :param entity: entity which should be deleted from database.
        :type entity: Type[BaseTable].
        :param conditions: mapping of columns to values which should be used as conditions during delete.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param returning: condition if deleted entities should be returned after delete.
        :type returning: bool, default False.
        :param session: session to use for deleting entries in database.
        :type session: Optional[AsyncSession], default None.
        :return: deleted entities or None.
        :rtype: Optional[Iterable[BaseTable]].
        """
        delete = partial(
            self._delete,
            entity=entity,
            conditions=conditions,
            returning=returning,
        )
        if not session:
            async with self.transaction() as session:
                return await delete(session=session)
        else:
            return await delete(session=session)

    async def _delete(
        self,
        entity: Type[BaseTable],
        session: AsyncSession,
        conditions: MutableMapping[Column, Any] = None,
        returning: bool = False,
    ) -> Optional[Iterable[BaseTable]]:
        """Delete from database given entity.

        :param entity: entity which should be deleted from database.
        :type entity: Type[BaseTable].
        :param session: session to use for deleting entries from database.
        :type session: AsyncSession.
        :param conditions: mapping of columns to values which should be used as conditions during delete.
        :type conditions: Optional[MutableMapping[Column, Any]], default None.
        :param returning: condition if deleted entities should be returned after delete.
        :type returning: bool, default False.
        :return: deleted entities or None.
        :rtype: Optional[Iterable[BaseTable]].
        """
        stmt = delete(entity)
        for column, value in (conditions or {}).items():
            stmt = stmt.where(column == value)
        if returning:
            stmt = stmt.returning(entity)

        result = await session.execute(stmt)

        if returning:
            entities = tuple(entity(**record) for record in result.fetchall())
            return entities
