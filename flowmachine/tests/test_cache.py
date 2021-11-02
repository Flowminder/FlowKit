# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for query caching functions.
"""

import pytest

from flowmachine.core.cache import (
    cache_table_exists,
    write_cache_metadata,
    get_obj_or_stub,
)
from flowmachine.core.context import get_db
from flowmachine.core.query import Query
from flowmachine.features import daily_location, ModalLocation, Flows


def test_table_records_removed(flowmachine_connect):
    """Test that removing a query from cache removes any Tables in cache that pointed to it."""
    dl = daily_location("2016-01-01")
    dl.store().result()
    assert dl.is_stored
    table = dl.get_table()
    assert cache_table_exists(get_db(), table.query_id)

    dl.invalidate_db_cache()
    assert not cache_table_exists(get_db(), table.query_id)


def test_do_cache_simple(flowmachine_connect):
    """
    Test that a simple object can be cached.

    """
    dl1 = daily_location("2016-01-01")
    write_cache_metadata(get_db(), dl1)
    assert cache_table_exists(get_db(), dl1.query_id)


def test_do_cache_multi(flowmachine_connect):
    """
    Test that a query containing subqueries can be cached.

    """

    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    write_cache_metadata(get_db(), hl1)

    assert cache_table_exists(get_db(), hl1.query_id)


def test_do_cache_nested(flowmachine_connect):
    """
    Test that a query containing nested subqueries can be cached.

    """
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl2 = ModalLocation(daily_location("2016-01-03"), daily_location("2016-01-04"))
    flow = Flows(hl1, hl2)
    write_cache_metadata(get_db(), flow)

    assert cache_table_exists(get_db(), flow.query_id)


def test_store_cache_simple(flowmachine_connect):
    """
    Test that storing a simple object also caches it.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    # Should be stored
    assert dl1.is_stored

    assert cache_table_exists(get_db(), dl1.query_id)


def test_store_cache_multi(flowmachine_connect):
    """
    Test that storing a query containing subqueries also caches it.

    """
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    hl1.store().result()
    # Should be stored
    assert hl1.is_stored

    assert cache_table_exists(get_db(), hl1.query_id)


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
    assert cache_table_exists(get_db(), flow.query_id)


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
    assert not cache_table_exists(get_db(), dl1.query_id)


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
    assert not cache_table_exists(get_db(), dl1.query_id)
    assert not cache_table_exists(get_db(), hl1.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert has_deps  # the remaining dependencies are due to underlying Table objects


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
    assert cache_table_exists(get_db(), dl1.query_id)
    assert not cache_table_exists(get_db(), hl1.query_id)
    assert not cache_table_exists(get_db(), flow.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert has_deps  # Daily location deps should remain


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
    assert not cache_table_exists(get_db(), dl1.query_id)
    assert cache_table_exists(get_db(), hl1.query_id)
    has_deps = bool(get_db().fetch("SELECT * FROM cache.dependencies"))
    assert has_deps


def test_deps_cache_multi():
    """
    Test that correct dependencies are returned.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    hl1 = ModalLocation(daily_location("2016-01-01"), daily_location("2016-01-02"))
    dep = dl1.query_id
    assert 4 == len(hl1._get_stored_dependencies())
    assert dep in [x.query_id for x in hl1._get_stored_dependencies()]


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
    bad_dep = dl1.query_id
    good_dep = hl1.query_id
    assert 6 == len(flow._get_stored_dependencies())
    assert good_dep in [x.query_id for x in flow._get_stored_dependencies()]
    assert bad_dep not in [x.query_id for x in flow._get_stored_dependencies()]


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
    dep = dl1.query_id
    assert 8 == len(flow._get_stored_dependencies())
    assert dep in [x.query_id for x in flow._get_stored_dependencies()]


def test_retrieve(get_dataframe):
    """
    Test that a query can be pulled back out of cache.

    """
    dl1 = daily_location("2016-01-01")
    dl1.store().result()
    local_df = get_dataframe(dl1)
    from_cache = {x.query_id: x for x in dl1.get_stored()}
    assert dl1.query_id in from_cache
    assert get_dataframe(from_cache[dl1.query_id]).equals(local_df)


@pytest.fixture
def create_and_store_novel_query():
    """Creates and stores a query object defined at runtime."""

    class TestQuery(Query):
        @property
        def column_names(self):
            return ["value"]

        def _make_query(self):
            return "select 1 as value"

    q = TestQuery()
    q_id = q.query_id
    q.store().result()
    yield q_id


def test_retrieve_novel_query(create_and_store_novel_query, flowmachine_connect):
    """ Test that a runtime defined query can be pulled from cache and invalidated. """
    from_cache = get_obj_or_stub(get_db(), create_and_store_novel_query)
    assert from_cache.query_id == create_and_store_novel_query
    assert from_cache.is_stored
    from_cache.invalidate_db_cache()
    assert not from_cache.is_stored


@pytest.fixture
def create_and_store_novel_query_with_dependency():
    """Creates and stores a query object defined at runtime with a composed query type."""

    class TestQuery(Query):
        @property
        def column_names(self):
            return ["value"]

        def _make_query(self):
            return "select 1 as value"

    class NestTestQuery(Query):
        def __init__(self):
            self.nested = TestQuery()
            super().__init__()

        @property
        def column_names(self):
            return ["value"]

        def _make_query(self):
            return "select 1 as value"

    q = NestTestQuery()
    q_id = q.query_id
    nested_id = q.nested.query_id
    q.store(store_dependencies=True).result()
    Query._QueryPool.clear()
    yield q_id, nested_id


def test_retrieve_novel_query_with_dependency(
    create_and_store_novel_query_with_dependency, flowmachine_connect
):
    """ Test that a runtime defined query composed of others can be pulled from cache and invalidated. """
    qid, nested_id = create_and_store_novel_query_with_dependency
    from_cache = get_obj_or_stub(get_db(), qid)
    assert from_cache.query_id == qid
    assert from_cache.is_stored
    assert from_cache.deps[0].query_id == nested_id
    assert type(from_cache).__name__ == "QStub"
    from_cache.deps[0].invalidate_db_cache(cascade=True)
    assert not from_cache.deps[0].is_stored
    assert not from_cache.is_stored


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

    from_cache = [obj.query_id for obj in Query.get_stored()]
    assert dl1.query_id in from_cache
    assert hl1.query_id in from_cache
    assert flow.query_id in from_cache
