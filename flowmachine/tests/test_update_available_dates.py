# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio

import pytest

from flowmachine.core import Query, Table
from flowmachine.core.cache import reset_cache
from flowmachine.core.server.server import update_available_dates


@pytest.mark.asyncio
async def test_available_dates_updates(flowmachine_connect):
    """
    Test the looping async task which updates the available dates on a loop
    as part of the server process.
    """
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
