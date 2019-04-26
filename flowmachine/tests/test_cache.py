# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for query caching functions.
"""

import pytest

from flowmachine.core.cache import cache_table_exists
from flowmachine.core.query import Query
from flowmachine.features import daily_location, ModalLocation, Flows


def test_table_records_removed(flowmachine_connect):
    """Test that removing a query from cache removes any Tables in cache that pointed to it."""
    dl = daily_location("2016-01-01")
    dl.store().result()
    assert dl.is_stored
    table = dl.get_table()
    assert cache_table_exists(flowmachine_connect, table.md5)

    dl.invalidate_db_cache()
    assert not cache_table_exists(flowmachine_connect, table.md5)


def test_do_cache_simple(flowmachine_connect):
    """
    Test that a simple object can be cached.

    """
    dl1 = daily_location("2016-01-01")
    dl1._db_store_cache_metadata()
    assert cache_table_exists(flowmachine_connect, dl1.md5)


def test_do_cache_multi(flowmachine_connect):
    """
    Test that a query containing subqueries can be cached.

    """

    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1._db_store_cache_metadata()

    assert cache_table_exists(flowmachine_connect, hl1.md5)


def test_do_cache_nested(flowmachine_connect):
    """
    Test that a query containing nested subqueries can be cached.

    """
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    flow._db_store_cache_metadata()

    assert cache_table_exists(flowmachine_connect, flow.md5)


def test_store_cache_simple(flowmachine_connect):
    """
    Test that storing a simple object also caches it.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    # Should be stored
    assert dl1.is_stored

    assert cache_table_exists(flowmachine_connect, dl1.md5)


def test_store_cache_multi(flowmachine_connect):
    """
    Test that storing a query containing subqueries also caches it.

    """
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    # Should be stored
    assert hl1.is_stored

    assert cache_table_exists(flowmachine_connect, hl1.md5)


def test_store_cache_nested(flowmachine_connect):
    """
    Test that storing a query with nested subqueries also caches it.

    """
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    flow.store().result()
    # Should be stored
    assert flow.is_stored
    assert cache_table_exists(flowmachine_connect, flow.md5)


def test_invalidate_cache_simple(flowmachine_connect):
    """
    Test that invalidating a simple object drops the table,
    and removes it from cache.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    assert dl1.is_stored
    dl1.invalidate_db_cache()
    assert not dl1.is_stored
    assert not cache_table_exists(flowmachine_connect, dl1.md5)


def test_invalidate_cache_multi(flowmachine_connect):
    """
    Test that invalidating a simple query that is part of
    a bigger one drops both tables, cleans up dependencies
    and removes both from cache.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    assert dl1.is_stored
    assert hl1.is_stored
    dl1.invalidate_db_cache()
    assert not dl1.is_stored
    assert not hl1.is_stored
    assert not cache_table_exists(flowmachine_connect, dl1.md5)
    assert not cache_table_exists(flowmachine_connect, hl1.md5)
    has_deps = bool(flowmachine_connect.fetch("SELECT * FROM cache.dependencies"))
    assert not has_deps


def test_invalidate_cache_midchain(flowmachine_connect):
    """
    Test that invalidating a query in the middle of a chain drops the
    top of the chain and this link, but not the bottom.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    flow.store().result()
    assert dl1.is_stored
    assert hl1.is_stored
    assert flow.is_stored
    hl1.invalidate_db_cache()
    assert dl1.is_stored
    assert not hl1.is_stored
    assert not flow.is_stored
    assert cache_table_exists(flowmachine_connect, dl1.md5)
    assert not cache_table_exists(flowmachine_connect, hl1.md5)
    assert not cache_table_exists(flowmachine_connect, flow.md5)
    has_deps = bool(flowmachine_connect.fetch("SELECT * FROM cache.dependencies"))
    assert has_deps  # Daily location deps should remain


def test_invalidate_cache_multi(flowmachine_connect):
    """
    Test that invalidating a simple query that is part of
    a bigger one drops both tables, cleans up dependencies
    and removes both from cache.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    assert dl1.is_stored
    assert hl1.is_stored
    dl1.invalidate_db_cache()
    assert not dl1.is_stored
    assert not hl1.is_stored
    assert not cache_table_exists(flowmachine_connect, dl1.md5)
    assert not cache_table_exists(flowmachine_connect, hl1.md5)
    has_deps = bool(flowmachine_connect.fetch("SELECT * FROM cache.dependencies"))
    assert has_deps


def test_invalidate_cascade(flowmachine_connect):
    """
    Test that invalidation does not cascade if cascade=False.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    flow.store().result()
    assert dl1.is_stored
    assert hl1.is_stored
    assert flow.is_stored
    dl1.invalidate_db_cache(cascade=False)
    assert not dl1.is_stored
    assert hl1.is_stored
    assert flow.is_stored
    assert not cache_table_exists(flowmachine_connect, dl1.md5)
    assert cache_table_exists(flowmachine_connect, hl1.md5)
    has_deps = bool(flowmachine_connect.fetch("SELECT * FROM cache.dependencies"))
    assert has_deps


def test_deps_cache_multi():
    """
    Test that correct dependencies are returned.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    dep = dl1.md5
    assert 3 == len(hl1._get_stored_dependencies())
    assert dep in [x.md5 for x in hl1._get_stored_dependencies()]


def test_deps_cache_chain():
    """
    Test that a Query -> cached1 -> cached2 chain will
    return only a dependency on cached1.

    """
    dl1 = daily_location("2016-01-01")
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    bad_dep = dl1.md5
    good_dep = hl1.md5
    assert 5 == len(flow._get_stored_dependencies())
    assert good_dep in [x.md5 for x in flow._get_stored_dependencies()]
    assert bad_dep not in [x.md5 for x in flow._get_stored_dependencies()]


def test_deps_cache_broken_chain():
    """
    Test that a Query -> not_cached -> cached chain will
    return a dependency on cached.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    dep = dl1.md5
    assert 7 == len(flow._get_stored_dependencies())
    assert dep in [x.md5 for x in flow._get_stored_dependencies()]


def test_retrieve(get_dataframe):
    """
    Test that a query can be pulled back out of cache.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    local_df = get_dataframe(dl1)
    from_cache = {x.md5: x for x in dl1.get_stored()}
    assert dl1.md5 in from_cache
    assert get_dataframe(from_cache[dl1.md5]).equals(local_df)


def test_df_not_pickled():
    """
    Test that a pickled query does not contain a dataframe.

    """
    import pickle

    dl1 = daily_location("2016-01-01")
    dl1.store().result()

    with pytest.raises(AttributeError):
        pickle.loads(pickle.dumps(dl1))._df


def test_retrieve_all():
    """
    Test that Query.get_stored returns everything.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    flow.store().result()

    from_cache = [obj.md5 for obj in Query.get_stored()]
    assert dl1.md5 in from_cache
    assert hl1.md5 in from_cache
    assert flow.md5 in from_cache
