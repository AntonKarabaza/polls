from typing import Optional

from common.meta import SingletonMeta
from connectors.db.postgres import PostgresDataService


class PollsDataService(PostgresDataService, metaclass=SingletonMeta):
    """Class for data manipulation for polls."""
    def __init__(
            self, host: str, port: int, database: str, user: str, password: str, schema: Optional[str] = None
    ):
        super().__init__(host, port, database, user, password, schema)

    async def get_questions(self):
        async with self.transaction() as session:
            data = await session.execute('SELECT * FROM question;')
            return tuple({key: val for key, val in row.items()} for row in data.mappings())

    async def get_choices(self):
        raise NotImplementedError
