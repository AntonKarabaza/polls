from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker


class BaseDataService:
    """Base class for data service."""
    _engine: AsyncEngine = None

    def __init__(self, url, **kwargs):
        if not self._engine:
            type(self)._engine: AsyncEngine = create_async_engine(url, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_pool()

    async def close_pool(self):
        await self._engine.dispose()

    @property
    def session(self) -> sessionmaker:
        return sessionmaker(self._engine, AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def transaction(self):
        async with self.session() as session:
            async with session.begin():
                yield session
                await session.commit()
