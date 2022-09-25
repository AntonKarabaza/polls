# Web application contexts
from modules.data_service.polls import PollsDataService


async def pg_context(app):
    """Controls initialization and disposal of application data service."""
    async with PollsDataService(**app['config']['db']['postgres']):
        yield
