import asyncio

import pytest

from flowmachine.core import Query, Table
from flowmachine.core.cache import reset_cache
from flowmachine.core.server.server import update_available_dates


@pytest.mark.asyncio
async def test_available_dates_updates(flowmachine_connect):
    reset_cache(flowmachine_connect, Query.redis, protect_table_objects=False)
    assert len(list(Table.get_stored())) == 0
    task = asyncio.create_task(
        update_available_dates(
            flowdb_connection=flowmachine_connect,
            pool=Query.thread_pool_executor,
            sleep_time=100,
            loop=False,
        )
    )
    avail = await task
    assert len(list(Table.get_stored())) == 15
