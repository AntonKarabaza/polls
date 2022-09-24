from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine

from connectors.db.base import BaseDataService


class PostgresDataService(BaseDataService):
    """PostgreSQL data service base."""
    _engine: AsyncEngine = None
    _url_template = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    def __init__(
        self, host: str, port: int, database: str, user: str, password: str, schema: Optional[str] = None
    ):
        self._db_url = self._url_template.format(
            user=user, password=password, host=host, port=port, database=database
        )
        self._schema = schema
        super().__init__(self._db_url, echo=True)
