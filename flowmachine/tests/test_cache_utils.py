# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for cache management utilities.
"""
from cachey import Scorer
from unittest.mock import Mock

import pytest

from flowmachine.core import Table
from flowmachine.core.cache import (
    compute_time,
    shrink_below_size,
    shrink_one,
    size_of_cache,
    size_of_table,
    score,
    get_query_object_by_id,
    get_cached_query_objects_ordered_by_score,
    touch_cache,
    get_max_size_of_cache,
    set_max_size_of_cache,
    get_cache_half_life,
    set_cache_half_life,
    invalidate_cache_by_id,
)
from flowmachine.features import daily_location


def test_scoring(flowmachine_connect):
    """Test that score updating algorithm is correct by comparing to cachey as reference implementation"""
    dl = daily_location("2016-01-01").store().result()
    dl_time = compute_time(flowmachine_connect, dl.md5)
    dl_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    initial_score = score(flowmachine_connect, dl.md5)
    cachey_scorer = Scorer(1000)
    cache_score = cachey_scorer.touch("dl", dl_time / dl_size)
    assert cache_score == pytest.approx(initial_score)
    # Touch again
    new_score = touch_cache(flowmachine_connect, dl.md5)
    updated_cache_score = cachey_scorer.touch("dl")
    assert updated_cache_score == pytest.approx(new_score)
    # Add another unrelated cache record, which should have a higher initial score
    dl_2 = daily_location("2016-01-02").store().result()
    dl_time = compute_time(flowmachine_connect, dl_2.md5)
    dl_size = size_of_table(flowmachine_connect, *dl_2.table_name.split(".")[::-1])
    cache_score = cachey_scorer.touch("dl_2", dl_time / dl_size)
    assert cache_score == pytest.approx(score(flowmachine_connect, dl_2.md5))


def test_touch_cache_record_for_query(flowmachine_connect):
    """Touching a cache record for a query should update access count, last accessed, & counter."""
    table = daily_location("2016-01-01").store().result()

    assert (
        1
        == flowmachine_connect.fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )
    accessed_at = flowmachine_connect.fetch(
        f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.md5}'"
    )[0][0]
    touch_cache(flowmachine_connect, table.md5)
    assert (
        2
        == flowmachine_connect.fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )
    # Two cache touches should have been recorded
    assert (
        4 == flowmachine_connect.fetch("SELECT nextval('cache.cache_touches');")[0][0]
    )
    assert (
        accessed_at
        < flowmachine_connect.fetch(
            f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )


def test_touch_cache_record_for_table(flowmachine_connect):
    """Touching a cache record for a table should update access count and last accessed but not touch score, or counter."""
    table = Table("events.calls_20160101")
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET compute_time = 1 WHERE query_id=%s", table.md5
    )  # Compute time for tables is zero, so set to 1 to avoid zeroing out
    assert 0 == score(flowmachine_connect, table.md5)
    assert (
        1
        == flowmachine_connect.fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )
    accessed_at = flowmachine_connect.fetch(
        f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.md5}'"
    )[0][0]
    touch_cache(flowmachine_connect, table.md5)
    assert 0 == score(flowmachine_connect, table.md5)
    assert (
        2
        == flowmachine_connect.fetch(
            f"SELECT access_count FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )
    # No cache touch should be recorded
    assert (
        2 == flowmachine_connect.fetch("SELECT nextval('cache.cache_touches');")[0][0]
    )
    assert (
        accessed_at
        < flowmachine_connect.fetch(
            f"SELECT last_accessed FROM cache.cached WHERE query_id='{table.md5}'"
        )[0][0]
    )


def test_compute_time():
    """Compute time should take value returned in ms and turn it into seconds."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = [[10]]
    assert 10 / 1000 == compute_time(connection_mock, "DUMMY_ID")


def test_get_cached_query_objects_ordered_by_score(flowmachine_connect):
    """Test that all records which are queries are returned in correct order."""
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    table = dl.get_table()
    # Should prefer the larger, but slower to calculate and more used dl over the aggregation
    cached_queries = get_cached_query_objects_ordered_by_score(flowmachine_connect)
    assert 2 == len(cached_queries)
    assert dl_agg.md5 == cached_queries[0][0].md5
    assert dl.md5 == cached_queries[1][0].md5
    assert 2 == len(cached_queries[0])


def test_shrink_one(flowmachine_connect):
    """Test that shrink_one removes a cache record."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 1 WHERE query_id='{dl_aggregate.md5}'"
    )
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.md5}'"
    )
    removed_query, table_size = shrink_one(flowmachine_connect)
    assert dl.md5 == removed_query.md5
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_shrink_to_size_does_nothing_when_cache_ok(flowmachine_connect):
    """Test that shrink_below_size doesn't remove anything if cache size is within limit."""
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        flowmachine_connect, size_of_cache(flowmachine_connect)
    )
    assert 0 == len(removed_queries)
    assert dl.is_stored


def test_shrink_to_size_removes_queries(flowmachine_connect):
    """Test that shrink_below_size removes queries when cache limit is breached."""
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        flowmachine_connect, size_of_cache(flowmachine_connect) - 1
    )
    assert 1 == len(removed_queries)
    assert not dl.is_stored


def test_shrink_to_size_respects_dry_run(flowmachine_connect):
    """Test that shrink_below_size doesn't remove anything during a dry run."""
    dl = daily_location("2016-01-01").store().result()
    dl2 = daily_location("2016-01-02").store().result()
    removed_queries = shrink_below_size(flowmachine_connect, 0, dry_run=True)
    assert 2 == len(removed_queries)
    assert dl.is_stored
    assert dl2.is_stored


def test_shrink_to_size_dry_run_reflects_wet_run(flowmachine_connect):
    """Test that shrink_below_size dry run is an accurate report."""
    dl = daily_location("2016-01-01").store().result()
    dl2 = daily_location("2016-01-02").store().result()
    shrink_to = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    queries_that_would_be_removed = shrink_below_size(
        flowmachine_connect, shrink_to, dry_run=True
    )
    removed_queries = shrink_below_size(flowmachine_connect, shrink_to, dry_run=False)
    assert [q.md5 for q in removed_queries] == [
        q.md5 for q in queries_that_would_be_removed
    ]


def test_shrink_to_size_uses_score(flowmachine_connect):
    """Test that shrink_below_size removes cache records in ascending score order."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 100 WHERE query_id='{dl_aggregate.md5}'"
    )
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.md5}'"
    )
    table_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    removed_queries = shrink_below_size(flowmachine_connect, table_size)
    assert 1 == len(removed_queries)
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_shrink_one(flowmachine_connect):
    """Test that shrink_one removes a cache record."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 100 WHERE query_id='{dl_aggregate.md5}'"
    )
    flowmachine_connect.engine.execute(
        f"UPDATE cache.cached SET cache_score_multiplier = 0.5 WHERE query_id='{dl.md5}'"
    )
    removed_query, table_size = shrink_one(flowmachine_connect)
    assert dl.md5 == removed_query.md5
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_size_of_cache(flowmachine_connect):
    """Test that cache size is reported correctly."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    total_cache_size = size_of_cache(flowmachine_connect)
    removed_query, table_size_a = shrink_one(flowmachine_connect)
    removed_query, table_size_b = shrink_one(flowmachine_connect)
    assert total_cache_size == table_size_a + table_size_b
    assert 0 == size_of_cache(flowmachine_connect)


def test_size_of_table(flowmachine_connect):
    """Test that table size is reported correctly."""
    dl = daily_location("2016-01-01").store().result()

    total_cache_size = size_of_cache(flowmachine_connect)
    table_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    assert total_cache_size == table_size


def test_cache_miss_value_error_rescore():
    """ValueError should be raised if we try to rescore something not in cache."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        touch_cache(connection_mock, "NOT_IN_CACHE")


def test_cache_miss_value_error_size_of_table():
    """ValueError should be raised if we try to get the size of something not in cache."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        size_of_table(connection_mock, "DUMMY_SCHEMA", "DUMMY_NAME")


def test_cache_miss_value_error_compute_time():
    """ValueError should be raised if we try to get the compute time of something not in cache."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        compute_time(connection_mock, "DUMMY_ID")


def test_cache_miss_value_error_score():
    """ValueError should be raised if we try to get the score of something not in cache."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = []
    with pytest.raises(ValueError):
        score(connection_mock, "DUMMY_ID")


def test_get_query_object_by_id(flowmachine_connect):
    """Test that we can get a query object back out of the database by the md5 id"""
    dl = daily_location("2016-01-01").store().result()
    retrieved_query = get_query_object_by_id(flowmachine_connect, dl.md5)
    assert dl.md5 == retrieved_query.md5
    assert dl.get_query() == retrieved_query.get_query()


def test_delete_query_by_id(flowmachine_connect):
    """Test that we can remove a query from cache by the md5 id"""
    dl = daily_location("2016-01-01").store().result()
    retrieved_query = invalidate_cache_by_id(flowmachine_connect, dl.md5)
    assert dl.md5 == retrieved_query.md5
    assert not dl.is_stored


def test_delete_query_by_id_does_not_cascade_by_default(flowmachine_connect):
    """Test that removing a query by id doesn't cascade by default"""
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    retrieved_query = invalidate_cache_by_id(flowmachine_connect, dl.md5)
    assert dl.md5 == retrieved_query.md5
    assert not dl.is_stored
    assert dl_agg.is_stored


def test_delete_query_by_id_can_cascade(flowmachine_connect):
    """Test that removing a query by id can cascade"""
    dl = daily_location("2016-01-01").store().result()
    dl_agg = dl.aggregate().store().result()
    retrieved_query = invalidate_cache_by_id(flowmachine_connect, dl.md5, cascade=True)
    assert dl.md5 == retrieved_query.md5
    assert not dl.is_stored
    assert not dl_agg.is_stored


@pytest.fixture
def reset_cache_settings(flowmachine_connect):
    """
    Fixture which ensures cache settings go back to what they were after
    they're manipulated.
    """
    max_cache_size = get_max_size_of_cache(flowmachine_connect)
    cache_half_life = get_cache_half_life(flowmachine_connect)
    yield flowmachine_connect
    set_max_size_of_cache(flowmachine_connect, max_cache_size)
    set_cache_half_life(flowmachine_connect, cache_half_life)


def test_get_set_cache_size_limit(reset_cache_settings):
    """Test that cache size can be got and set"""
    # Initial setting depends on the disk space of the FlowDB container so just check it is nonzero
    assert get_max_size_of_cache(reset_cache_settings) > 0
    # Now set it to something
    set_max_size_of_cache(reset_cache_settings, 10)
    assert 10 == get_max_size_of_cache(reset_cache_settings)


def test_get_set_cache_half_life(reset_cache_settings):
    """Test that cache halflife can be got and set"""
    assert 1000 == get_cache_half_life(reset_cache_settings)
    # Now set it to something
    set_cache_half_life(reset_cache_settings, 10)
    assert 10 == get_cache_half_life(reset_cache_settings)
