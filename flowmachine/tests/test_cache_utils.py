# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for cache management utilities.
"""
from unittest.mock import Mock

import pytest

from flowmachine.core.cache import (
    compute_time,
    shrink_below_size,
    shrink_one,
    size_of_cache,
    size_of_table,
    score,
    get_query_by_id,
    get_cached_queries_by_score,
)
from flowmachine.features import daily_location


def test_rescore(flowmachine_connect):
    """Test that score updating algorithm is correct"""
    dl = daily_location("2016-01-01").store().result()
    dl_compute_time = compute_time(flowmachine_connect, dl.md5)
    dl_disk_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    new_score = score(flowmachine_connect, dl.md5, half_life=2)
    assert dl_compute_time / dl_disk_size * (1 + 2) == pytest.approx(new_score)
    dl.get_query()  # Should increment the cache score
    new_score = score(flowmachine_connect, dl.md5, half_life=2)
    assert 2 * (dl_compute_time / dl_disk_size * (1 + 2)) == pytest.approx(new_score)


def test_compute_time():
    """Compute time should take value returned in ms and turn it into seconds."""
    connection_mock = Mock()
    connection_mock.fetch.return_value = [[10]]
    assert 10 / 1000 == compute_time(connection_mock, "DUMMY_ID")


def test_get_cached_queries_by_score(flowmachine_connect):
    """Test that all records which are queries are returned in correct order."""
    dl = daily_location("2016-01-01").store().result()
    dl_big = daily_location("2016-01-01", level="lat-lon").store().result()
    table = dl.get_table()
    cached_queries = get_cached_queries_by_score(flowmachine_connect, 1000)
    assert 2 == len(cached_queries)
    assert dl_big.md5 == cached_queries[0][0].md5
    assert dl.md5 == cached_queries[1][0].md5
    assert 2 == len(cached_queries[0])


def test_shrink_one(flowmachine_connect):
    """Test that shrink_one removes a cache record."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    removed_query, table_size = shrink_one(flowmachine_connect)
    assert dl.md5 == removed_query.md5
    assert not dl.is_stored
    assert dl_aggregate.is_stored


def test_shrink_to_size_does_nothing_when_cache_ok(flowmachine_connect):
    """Test that shrink_below_size doesn't remove anything if cache size is within limit."""
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        flowmachine_connect, size_of_cache(flowmachine_connect), 1000
    )
    assert 0 == len(removed_queries)
    assert dl.is_stored


def test_shrink_to_size_removes_queries(flowmachine_connect):
    """Test that shrink_below_size removes queries when cache limit is breached."""
    dl = daily_location("2016-01-01").store().result()
    removed_queries = shrink_below_size(
        flowmachine_connect, size_of_cache(flowmachine_connect) - 1, 1000
    )
    assert 1 == len(removed_queries)
    assert not dl.is_stored


def test_shrink_to_size_respects_dry_run(flowmachine_connect):
    """Test that shrink_below_size doesn't remove anything during a dry run."""
    dl = daily_location("2016-01-01").store().result()
    dl2 = daily_location("2016-01-02").store().result()
    removed_queries = shrink_below_size(flowmachine_connect, 0, 1000, dry_run=True)
    assert 2 == len(removed_queries)
    assert dl.is_stored
    assert dl2.is_stored


def test_shrink_to_size_uses_score(flowmachine_connect):
    """Test that shrink_below_size removes cache records in ascending score order."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    table_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    removed_queries = shrink_below_size(flowmachine_connect, table_size, 1000)
    assert 1 == len(removed_queries)
    assert dl.is_stored
    assert not dl_aggregate.is_stored


def test_shrink_one(flowmachine_connect):
    """Test that shrink_one removes a cache record."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    removed_query, table_size = shrink_one(flowmachine_connect, 1000)
    assert dl_aggregate.md5 == removed_query.md5
    assert dl.is_stored
    assert not dl_aggregate.is_stored


def test_size_of_cache(flowmachine_connect):
    """Test that cache size is reported correctly."""
    dl = daily_location("2016-01-01").store().result()
    dl_aggregate = dl.aggregate().store().result()
    total_cache_size = size_of_cache(flowmachine_connect)
    removed_query, table_size_a = shrink_one(flowmachine_connect, 1000)
    removed_query, table_size_b = shrink_one(flowmachine_connect, 1000)
    assert total_cache_size == table_size_a + table_size_b
    assert 0 == size_of_cache(flowmachine_connect)


def test_size_of_table(flowmachine_connect):
    """Test that table size is reported correctly."""
    dl = daily_location("2016-01-01").store().result()

    total_cache_size = size_of_cache(flowmachine_connect)
    table_size = size_of_table(flowmachine_connect, *dl.table_name.split(".")[::-1])
    assert total_cache_size == table_size


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
        score(connection_mock, "DUMMY_ID", 1000)


def test_get_query_by_id(flowmachine_connect):
    """Test that we can get a query object back out of the database by the md5 id"""
    dl = daily_location("2016-01-01").store().result()
    retrieved_query = get_query_by_id(flowmachine_connect, dl.md5)
    assert dl.md5 == retrieved_query.md5
    assert dl.get_query() == retrieved_query.get_query()
