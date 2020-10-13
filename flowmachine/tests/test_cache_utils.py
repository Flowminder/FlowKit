# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for cache management utilities.
"""
from cachey import Scorer
from unittest.mock import Mock

import pytest

from flowmachine.core import Table, Query
from flowmachine.core.cache import (
    get_compute_time,
    shrink_below_size,
    shrink_one,
    get_size_of_cache,
    get_size_of_table,
    get_score,
    get_query_object_by_id,
    get_cached_query_objects_ordered_by_score,
    touch_cache,
    get_max_size_of_cache,
    set_max_size_of_cache,
    get_cache_half_life,
    set_cache_half_life,
    invalidate_cache_by_id,
    cache_table_exists,
    resync_redis_with_cache,
    reset_cache,
    write_cache_metadata,
    write_query_to_cache,
    get_cache_protected_period,
    set_cache_protected_period,
    watch_and_shrink_cache,
)
from flowmachine.core.context import get_db, get_redis, get_executor
from flowmachine.core.query_state import QueryState, QueryStateMachine
from flowmachine.features import daily_location


class TestException(Exception):
    """
    Exception only for use in these tests..
    """

    pass


def test_scoring(flowmachine_connect):
    """
    Test that score updating algorithm is correct by comparing to cachey as reference implementation
    """
    dl = daily_location("2016-01-01").store().result()
    dl_time = get_compute_time(get_db(), dl.query_id)
    dl_size = get_size_of_table(get_db(), dl.table_name, "cache")
    initial_score = get_score(get_db(), dl.query_id)
    cachey_scorer = Scorer(halflife=1000.0)
    cache_score = cachey_scorer.touch("dl", dl_time / dl_size)
    assert cache_score == pytest.approx(initial_score)

    # Touch again
    new_score = touch_cache(get_db(), dl.query_id)
    updated_cache_score = cachey_scorer.touch("dl")
    assert updated_cache_score == pytest.approx(new_score)

    # Add another unrelated cache record, which should have a higher initial score
    dl_2 = daily_location("2016-01-02").store().result()
    dl_time = get_compute_time(get_db(), dl_2.query_id)
    dl_size = get_size_of_table(get_db(), dl_2.table_name, "cache")
    cache_score = cachey_scorer.touch("dl_2", dl_time / dl_size)
    assert cache_score == pytest.approx(get_score(get_db(), dl_2.query_id))


def test_touch_cache_record_for_query(flowmachine_connect):
    """
    Touching a cache record for a query should update access count, last accessed, & counter.
    """
    table = daily_location("2016-01-01").store().result()

    assert (
        1
        == get_db().fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )
    accessed_at = get_db().fetch(
        f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.query_id}'"
    )[0][0]
    touch_cache(get_db(), table.query_id)
    assert (
        2
        == get_db().fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )
    # Two cache touches should have been recorded
    assert 4 == get_db().fetch("SELECT nextval('cache.cache_touches');")[0][0]
    assert (
        accessed_at
        < get_db().fetch(
            f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )


def test_touch_cache_record_for_table(flowmachine_connect):
    """
    Touching a cache record for a table should update access count and last accessed but not touch score, or counter.
    """
    table = Table("events.calls_20160101")
    get_db().engine.execute(
        f"UPDATE cache.cached SET compute_time = 1 WHERE query_id=%s", table.query_id
    )  # Compute time for tables is zero, so set to 1 to avoid zeroing out
    assert 0 == get_score(get_db(), table.query_id)
    assert (
        1
        == get_db().fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )
    accessed_at = get_db().fetch(
        f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.query_id}'"
    )[0][0]
    touch_cache(get_db(), table.query_id)
    assert 0 == get_score(get_db(), table.query_id)
    assert (
        2
        == get_db().fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )
    # No cache touch should be recorded
    assert 2 == get_db().fetch("SELECT nextval('cache.cache_touches');")[0][0]
    assert (
        accessed_at
        < get_db().fetch(
            f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.query_id}'"
        )[0][0]
    )


def test_get_compute_time():
    """
    Compute time should take value returned in ms and turn it into seconds.
    """
    connection_mock = Mock()
    connection_mock.fetch.return_value = [[10]]
    assert 10 / 1000 == get_compute_time(connection_mock, "DUMMY_ID")


def test_get_cached_query_objects_ordered_by_score(flowmachine_connect):
    """
    Test that all records which are queries are returned in correct order.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    table = dl.get_table()

    # Should prefer the larger, but slower to calculate and more used dl over the aggregation
    cached_queries = list(
        get_cached_query_objects_ordered_by_score(get_db(), protected_period=-1)
    )
    assert 2 == len(cached_queries)
    assert dl_agg.query_id == cached_queries[0][0].query_id
    assert dl.query_id == cached_queries[1][0].query_id
    assert 2 == len(cached_queries[0])


def test_get_cached_query_objects_protected_period(flowmachine_connect):
    """
    Test that all records which are queries are returned in correct order, but queries within protected period are omitted.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    table = dl.get_table()

    cached_queries = list(
        get_cached_query_objects_ordered_by_score(get_db(), protected_period=-1)
    )
    assert 2 == len(cached_queries)
    assert dl_agg.query_id == cached_queries[0][0].query_id
    assert dl.query_id == cached_queries[1][0].query_id
    assert 2 == len(cached_queries[0])
    cached_queries = list(
        get_cached_query_objects_ordered_by_score(get_db(), protected_period=10)
    )
    assert 0 == len(cached_queries)


def test_shrink_one(flowmachine_connect):
    """
    Test that shrink_one removes a cache record.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 1 WHERE query_id='{dl_aggregate.query_id}'"
    )
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.query_id}'"
    )
    removed_query, table_size = shrink_one(get_db(), protected_period=-1)
    assert dl.query_id == removed_query.query_id
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_shrink_to_size_does_nothing_when_cache_ok(flowmachine_connect):
    """
    Test that shrink_below_size doesn't remove anything if cache size is within limit.
    """
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        get_db(), get_size_of_cache(get_db()), protected_period=-1
    )
    assert 0 == len(removed_queries)
    assert dl.is_stored


def test_shrink_to_size_removes_queries(flowmachine_connect):
    """
    Test that shrink_below_size removes queries when cache limit is breached.
    """
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        get_db(),
        get_size_of_cache(get_db()) - 1,
        protected_period=-1,
    )
    assert 1 == len(removed_queries)
    assert not dl.is_stored


def test_shrink_to_size_respects_dry_run(flowmachine_connect):
    """
    Test that shrink_below_size doesn't remove anything during a dry run.
    """
    dl = daily_location("2016-01-01").store().result()
    dl2 = daily_location("2016-01-02").store().result()
    removed_queries = shrink_below_size(get_db(), 0, dry_run=True, protected_period=-1)
    assert 2 == len(removed_queries)
    assert dl.is_stored
    assert dl2.is_stored


def test_shrink_to_size_dry_run_reflects_wet_run(flowmachine_connect):
    """
    Test that shrink_below_size dry run is an accurate report.
    """
    dl = daily_location("2016-01-01").store().result()
    daily_location("2016-01-02").store().result()
    daily_location("2016-01-03").store().result()

    shrink_to = get_size_of_table(get_db(), dl.table_name, "cache")
    queries_that_would_be_removed = shrink_below_size(
        get_db(), shrink_to, dry_run=True, protected_period=-1
    )
    removed_queries = shrink_below_size(
        get_db(), shrink_to, dry_run=False, protected_period=-1
    )
    assert [q.query_id for q in removed_queries] == [
        q.query_id for q in queries_that_would_be_removed
    ]


def test_shrink_to_size_uses_score(flowmachine_connect):
    """
    Test that shrink_below_size removes cache records in ascending score order.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 100 WHERE query_id='{dl_aggregate.query_id}'"
    )
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.query_id}'"
    )
    table_size = get_size_of_table(get_db(), dl.table_name, "cache")
    removed_queries = shrink_below_size(get_db(), table_size, protected_period=-1)
    assert 1 == len(removed_queries)
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_shrink_one(flowmachine_connect):
    """
    Test that shrink_one removes a cache record.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 100 WHERE query_id='{dl_aggregate.query_id}'"
    )
    get_db().engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.query_id}'"
    )
    removed_query, table_size = shrink_one(get_db(), protected_period=-1)
    assert dl.query_id == removed_query.query_id
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_size_of_cache(flowmachine_connect):
    """
    Test that cache size is reported correctly.
    """
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    total_cache_size = get_size_of_cache(get_db())
    removed_query, table_size_a = shrink_one(get_db(), protected_period=-1)
    removed_query, table_size_b = shrink_one(get_db(), protected_period=-1)
    assert total_cache_size == table_size_a + table_size_b
    assert 0 == get_size_of_cache(get_db())


def test_size_of_table(flowmachine_connect):
    """
    Test that table size is reported correctly.
    """
    dl = daily_location("2016-01-01").store().result()

    total_cache_size = get_size_of_cache(get_db())
    table_size = get_size_of_table(get_db(), dl.table_name, "cache")
    assert total_cache_size == table_size


def test_cache_miss_value_error_rescore():
    """
    ValueError should be raised if we try to rescore something not in cache.
    """
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        touch_cache(connection_mock, "NOT_IN_CACHE")


def test_cache_miss_value_error_size_of_table():
    """
    ValueError should be raised if we try to get the size of something not in cache.
    """
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        get_size_of_table(connection_mock, "DUMMY_SCHEMA", "DUMMY_NAME")


def test_cache_miss_value_error_compute_time():
    """
    ValueError should be raised if we try to get the compute time of something not in cache.
    """
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        get_compute_time(connection_mock, "DUMMY_ID")


def test_cache_miss_value_error_score():
    """
    ValueError should be raised if we try to get the score of something not in cache.
    """
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        get_score(connection_mock, "DUMMY_ID")


def test_get_query_object_by_id(flowmachine_connect):
    """
    Test that we can get a query object back out of the database by the query's id
    """
    dl = daily_location("2016-01-01").store().result()
    retrieved_query = get_query_object_by_id(get_db(), dl.query_id)
    assert dl.query_id == retrieved_query.query_id
    assert dl.get_query() == retrieved_query.get_query()


def test_delete_query_by_id(flowmachine_connect):
    """
    Test that we can remove a query from cache by the query's id
    """
    dl = daily_location("2016-01-01").store().result()
    retrieved_query = invalidate_cache_by_id(get_db(), dl.query_id)
    assert dl.query_id == retrieved_query.query_id
    assert not dl.is_stored


def test_delete_query_by_id_does_not_cascade_by_default(flowmachine_connect):
    """
    Test that removing a query by id doesn't cascade by default
    """
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    retrieved_query = invalidate_cache_by_id(get_db(), dl.query_id)
    assert dl.query_id == retrieved_query.query_id
    assert not dl.is_stored
    assert dl_agg.is_stored


def test_delete_query_by_id_can_cascade(flowmachine_connect):
    """
    Test that removing a query by id can cascade
    """
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    retrieved_query = invalidate_cache_by_id(get_db(), dl.query_id, cascade=True)
    assert dl.query_id == retrieved_query.query_id
    assert not dl.is_stored
    assert not dl_agg.is_stored


@pytest.fixture
def flowmachine_connect_with_cache_settings_reset(flowmachine_connect):
    """
    Fixture which ensures cache settings go back to what they were after
    they're manipulated.
    """
    max_cache_size = get_max_size_of_cache(get_db())
    cache_half_life = get_cache_half_life(get_db())
    cache_protect_period = get_cache_protected_period(get_db())
    yield
    set_max_size_of_cache(get_db(), max_cache_size)
    set_cache_half_life(get_db(), cache_half_life)
    set_cache_protected_period(get_db(), cache_protect_period)


def test_get_set_cache_size_limit(flowmachine_connect_with_cache_settings_reset):
    """
    Test that cache size can be got and set
    """
    # Initial setting depends on the disk space of the FlowDB container so just check it is nonzero
    assert get_max_size_of_cache(get_db()) > 0
    # Now set it to something
    set_max_size_of_cache(get_db(), 10)
    assert 10 == get_max_size_of_cache(get_db())


def test_get_set_cache_half_life(flowmachine_connect_with_cache_settings_reset):
    """
    Test that cache halflife can be got and set
    """
    assert 1000 == get_cache_half_life(get_db())
    # Now set it to something
    set_cache_half_life(get_db(), 10)
    assert 10 == get_cache_half_life(get_db())


def test_cache_table_exists(flowmachine_connect):
    """
    Test that cache_table_exists reports accurately.
    """
    assert not cache_table_exists(get_db(), "NONEXISTENT_CACHE_ID")
    assert cache_table_exists(
        get_db(), daily_location("2016-01-01").store().result().query_id
    )


def test_redis_resync(flowmachine_connect):
    """
    Test that redis states can be resynced to the flowdb cache.
    """
    stored_query = daily_location("2016-01-01").store().result()
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.COMPLETED
    )
    assert stored_query.is_stored
    get_redis().flushdb()
    assert stored_query.is_stored
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.KNOWN
    )
    resync_redis_with_cache(get_db(), get_redis())
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.COMPLETED
    )


def test_cache_reset(flowmachine_connect):
    """
    Test that cache and redis are both reset.
    """
    stored_query = daily_location("2016-01-01").store().result()
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.COMPLETED
    )
    assert stored_query.is_stored
    reset_cache(get_db(), get_redis())
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.KNOWN
    )
    assert not stored_query.is_stored


def test_cache_reset_protects_tables(flowmachine_connect):
    """
    Resetting the cache should preserve Table entries.
    """
    # Regression test for https://github.com/Flowminder/FlowKit/issues/832
    dl_query = daily_location(date="2016-01-03", method="last")
    reset_cache(get_db(), get_redis())
    for dep in dl_query._get_stored_dependencies():
        assert dep.query_id in [x.query_id for x in Query.get_stored()]
    dl_query.store().result()  # Original bug caused this to error


def test_redis_resync_runtimeerror(flowmachine_connect, dummy_redis):
    """
    Test that a runtime error is raised if redis is being updated from multiple places when trying to resync.
    """
    stored_query = daily_location("2016-01-01").store().result()
    assert (
        QueryStateMachine(
            get_redis(), stored_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.COMPLETED
    )
    dummy_redis.allow_flush = False
    with pytest.raises(RuntimeError):
        resync_redis_with_cache(get_db(), dummy_redis)


def test_cache_metadata_write_error(flowmachine_connect, dummy_redis, monkeypatch):
    """
    Test that errors during cache metadata writing leave the query state machine in error state.
    """
    # Regression test for https://github.com/Flowminder/FlowKit/issues/833

    writer_mock = Mock(side_effect=TestException)
    dl_query = daily_location(date="2016-01-03", method="last")
    assert not dl_query.is_stored
    monkeypatch.setattr("flowmachine.core.cache.write_cache_metadata", writer_mock)

    store_future = dl_query.store()
    with pytest.raises(TestException):
        store_future.result()
    assert not dl_query.is_stored
    assert (
        QueryStateMachine(
            get_redis(), dl_query.query_id, get_db().conn_id
        ).current_query_state
        == QueryState.ERRORED
    )


def test_cache_ddl_op_error(dummy_redis):
    """
    Test that errors when generating SQL leave the query state machine in error state.
    """

    query_mock = Mock(query_id="DUMMY_MD5")
    qsm = QueryStateMachine(dummy_redis, "DUMMY_MD5", "DUMMY_CONNECTION")
    qsm.enqueue()

    with pytest.raises(TestException):
        write_query_to_cache(
            name="DUMMY_QUERY",
            redis=dummy_redis,
            query=query_mock,
            connection=Mock(conn_id="DUMMY_CONNECTION"),
            ddl_ops_func=Mock(side_effect=TestException),
            write_func=Mock(),
        )
    assert qsm.current_query_state == QueryState.ERRORED


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
