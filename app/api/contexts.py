from modules.data_service.polls import PollsDataService


async def pg_context(app):
    async with PollsDataService(**app['config']['db']['postgres']):
        yield
