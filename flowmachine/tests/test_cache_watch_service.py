# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for cache shrink service.
"""

import pytest

from flowmachine.core.cache import get_size_of_cache
from flowmachine.core.context import get_db, get_executor
from flowmachine.core.server.cache_cleanup import watch_and_shrink_cache
from flowmachine.features import daily_location


@pytest.mark.asyncio
async def test_cache_watch_does_shrink(flowmachine_connect):
    """
    Test that the cache watcher will shrink cache tables.
    """
    dl = daily_location("2016-01-01").store().result()
    assert dl.is_stored
    assert get_size_of_cache(get_db()) > 0
    await watch_and_shrink_cache(
        flowdb_connection=get_db(),
        pool=get_executor(),
        sleep_time=0,
        loop=False,
        protected_period=-1,
        size_threshold=1,
    )
    assert not dl.is_stored
    assert get_size_of_cache(get_db()) == 0


@pytest.mark.asyncio
async def test_cache_watch_does_timeout(flowmachine_connect, json_log):
    """
    Test that the cache watcher will timeout and log that it has.
    """
    await watch_and_shrink_cache(
        flowdb_connection=get_db(),
        pool=get_executor(),
        sleep_time=0,
        loop=False,
        protected_period=-1,
        size_threshold=1,
        timeout=0,
    )
    log_lines = [x for x in json_log().err if x["level"] == "error"]
    assert (
        log_lines[0]["event"]
        == "Failed to complete cache shrink within 0s. Trying again in 0s."
    )
