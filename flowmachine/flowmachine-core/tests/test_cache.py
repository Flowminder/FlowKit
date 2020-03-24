# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for query caching functions.
"""

import pytest

from flowmachine_core.core.cache import (
    cache_table_exists,
    write_cache_metadata,
)
from flowmachine_core.core.context import get_db
from flowmachine_core.query_bases.query import Query


def test_table_records_removed(test_query):
    """Test that removing a query from cache removes any Tables in cache that pointed to it."""
    test_query.store().result()
    assert test_query.is_stored
    table = test_query.get_table()
    assert cache_table_exists(get_db(), table.query_id)

    test_query.invalidate_db_cache()
    assert not cache_table_exists(get_db(), table.query_id)


def test_do_cache_simple(test_query):
    """
    Test that a simple object can be cached.

    """
    write_cache_metadata(get_db(), test_query)
    assert cache_table_exists(get_db(), test_query.query_id)


def test_do_cache_multi(nested_test_query):
    """
    Test that a query containing subqueries can be cached.

    """

    write_cache_metadata(get_db(), nested_test_query)

    assert cache_table_exists(get_db(), nested_test_query.query_id)


def test_do_cache_nested(deeply_nested_test_query):
    """
    Test that a query containing nested subqueries can be cached.

    """
    write_cache_metadata(get_db(), deeply_nested_test_query)

    assert cache_table_exists(get_db(), deeply_nested_test_query.query_id)


def test_store_cache_simple(test_query):
    """
    Test that storing a simple object also caches it.

    """
    test_query.store().result()
    # Should be stored
    assert test_query.is_stored

    assert cache_table_exists(get_db(), test_query.query_id)


def test_store_cache_multi(nested_test_query):
    """
    Test that storing a query containing subqueries also caches it.

    """
    nested_test_query.store().result()
    # Should be stored
    assert nested_test_query.is_stored

    assert cache_table_exists(get_db(), nested_test_query.query_id)


def test_store_cache_nested(deeply_nested_test_query):
    """
    Test that storing a query with nested subqueries also caches it.

    """

    deeply_nested_test_query.store().result()
    # Should be stored
    assert deeply_nested_test_query.is_stored
    assert cache_table_exists(get_db(), deeply_nested_test_query.query_id)


def test_invalidate_cache_simple(test_query):
    """
    Test that invalidating a simple object drops the table,
    and removes it from cache.

    """
    test_query.store().result()
    assert test_query.is_stored
    test_query.invalidate_db_cache()
    assert not test_query.is_stored
    assert not cache_table_exists(get_db(), test_query.query_id)


def test_invalidate_cache_multi(test_query, nested_test_query):
    """
    Test that invalidating a simple query that is part of
    a bigger one drops both tables, cleans up dependencies
    and removes both from cache.

    """
    test_query.store().result()
    nested_test_query.store().result()
    assert test_query.is_stored
    assert nested_test_query.is_stored
    test_query.invalidate_db_cache()
    assert not test_query.is_stored
    assert not nested_test_query.is_stored
    assert not cache_table_exists(get_db(), test_query.query_id)
    assert not cache_table_exists(get_db(), nested_test_query.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert (
        not has_deps
    )  # the remaining dependencies are due to underlying Table objects


def test_invalidate_cache_midchain(
    test_query, nested_test_query, deeply_nested_test_query
):
    """
    Test that invalidating a query in the middle of a chain drops the
    top of the chain and this link, but not the bottom.

    """
    test_query.store().result()

    nested_test_query.store().result()
    deeply_nested_test_query.store().result()
    assert test_query.is_stored
    assert nested_test_query.is_stored
    assert deeply_nested_test_query.is_stored
    nested_test_query.invalidate_db_cache()
    assert test_query.is_stored
    assert not nested_test_query.is_stored
    assert not deeply_nested_test_query.is_stored
    assert cache_table_exists(get_db(), test_query.query_id)
    assert not cache_table_exists(get_db(), nested_test_query.query_id)
    assert not cache_table_exists(get_db(), deeply_nested_test_query.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert not has_deps  # Daily location deps should remain


def test_invalidate_cascade(test_query, nested_test_query, deeply_nested_test_query):
    """
    Test that invalidation does not cascade if cascade=False.

    """
    test_query.store().result()
    nested_test_query.store().result()
    deeply_nested_test_query.store().result()
    assert test_query.is_stored
    assert nested_test_query.is_stored
    assert deeply_nested_test_query.is_stored
    test_query.invalidate_db_cache(cascade=False)
    assert not test_query.is_stored
    assert nested_test_query.is_stored
    assert deeply_nested_test_query.is_stored
    assert not cache_table_exists(get_db(), test_query.query_id)
    assert cache_table_exists(get_db(), nested_test_query.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert has_deps


def test_deps_cache_multi(test_query, nested_test_query):
    """
    Test that correct dependencies are returned.

    """
    test_query.store().result()
    dep = test_query.query_id
    assert 1 == len(nested_test_query._get_stored_dependencies())
    assert dep in [x.query_id for x in nested_test_query._get_stored_dependencies()]


def test_deps_cache_chain(test_query, nested_test_query, deeply_nested_test_query):
    """
    Test that a Query -> cached1 -> cached2 chain will
    return only a dependency on cached1.

    """

    nested_test_query.store().result()
    bad_dep = test_query.query_id
    good_dep = nested_test_query.query_id
    assert 1 == len(deeply_nested_test_query._get_stored_dependencies())
    assert good_dep in [
        x.query_id for x in deeply_nested_test_query._get_stored_dependencies()
    ]
    assert bad_dep not in [
        x.query_id for x in deeply_nested_test_query._get_stored_dependencies()
    ]


def test_deps_cache_broken_chain(
    test_query, nested_test_query, deeply_nested_test_query
):
    """
    Test that a Query -> not_cached -> cached chain will
    return a dependency on cached.

    """
    test_query.store().result()
    dep = test_query.query_id
    assert 1 == len(deeply_nested_test_query._get_stored_dependencies())
    assert dep in [
        x.query_id for x in deeply_nested_test_query._get_stored_dependencies()
    ]


def test_retrieve(get_dataframe, test_query):
    """
    Test that a query can be pulled back out of cache.

    """
    test_query.store().result()
    local_df = get_dataframe(test_query)
    from_cache = {x.query_id: x for x in test_query.get_stored()}
    assert test_query.query_id in from_cache
    assert get_dataframe(from_cache[test_query.query_id]).equals(local_df)


def test_df_not_pickled(test_query):
    """
    Test that a pickled query does not contain a dataframe.

    """
    import pickle

    test_query.store().result()

    with pytest.raises(AttributeError):
        pickle.loads(pickle.dumps(test_query))._df


def test_retrieve_all(test_query, nested_test_query, deeply_nested_test_query):
    """
    Test that Query.get_stored returns everything.

    """
    test_query.store().result()

    nested_test_query.store().result()

    deeply_nested_test_query.store().result()

    from_cache = [obj.query_id for obj in Query.get_stored()]
    assert test_query.query_id in from_cache
    assert nested_test_query.query_id in from_cache
    assert deeply_nested_test_query.query_id in from_cache
