import pytest
from unittest.mock import Mock

from flowmachine.core.query_state import QueryStateMachine, QueryState
from flowmachine.core.server.query_proxy import (
    QueryProxy,
    QueryProxyError,
    MissingQueryError,
)
from flowmachine.core.query import Query
from flowmachine.features import daily_location


def test_construct_query_proxy():
    """
    Can construct QueryProxy from query_kind and parameters.
    """
    query_proxy1 = QueryProxy("foo_query", {"param": "quux"})
    query_proxy2 = QueryProxy(
        "bar_query", {"param1": "some_value", "param2": "another_value"}
    )

    assert query_proxy1.query_kind == "foo_query"
    assert query_proxy2.query_kind == "bar_query"
    assert query_proxy1.params == {"param": "quux"}
    assert query_proxy2.params == {"param1": "some_value", "param2": "another_value"}


def test_wrong_argument_types():
    query_kind_1 = daily_location
    params_1 = {
        "date": "2016-01-01",
        "daily_location_method": "last",
        "aggregation_unit": "admin2",
        "subscriber_subset": "all",
    }

    with pytest.raises(
        QueryProxyError, match="Argument 'query_kind' must be of type str"
    ):
        _ = QueryProxy(query_kind_1, params_1)

    query_kind_2 = "daily_location"
    params_2 = ["2016-01-01", "last", "admin2", "all"]

    with pytest.raises(QueryProxyError, match="Argument 'params' must be of type dict"):
        _ = QueryProxy(query_kind_2, params_2)


def test_run_query_async(dummy_redis):
    """
    Running run_query_async() creates the expected redis keys and calls store() on the underlying query object.
    """

    # Define mock query object and a function which returns it when called.
    # This serves as a drop-in replacement for 'construct_query_object' in
    # flowmachine.core.server.query_proxy.
    q = Mock()
    q.md5 = "dummy_query_id_non_aggregate"
    q.aggregate().md5 = "dummy_query_id_aggregate"

    def dummy_construct_query_object(query_kind, params):
        return q

    # Construct query proxy
    query_proxy = QueryProxy(
        "dummy_query",
        {"param1": "some_value", "param2": "another_value"},
        redis=dummy_redis,
        func_construct_query_object=dummy_construct_query_object,
    )
    assert dummy_redis.keys() == []

    # Run the query and obtain the resulting query id
    query_id = query_proxy.run_query_async()

    # Check that redis contains the expected keys q.store() has been called
    expected_redis_keys = [
        "dummy_query_id_aggregate",
        '{"params": {"param1": "some_value", "param2": "another_value"}, "query_kind": "dummy_query"}',
    ]
    assert expected_redis_keys == dummy_redis.keys()
    assert "dummy_query_id_aggregate" == query_id
    q.store.assert_called_once_with()


def test_poll(dummy_redis, monkeypatch):
    """
    Running poll() returns the expected status.
    """
    # Define mock query object and a function which returns it when called.
    # This serves as a drop-in replacement for 'construct_query_object' in
    # flowmachine.core.server.query_proxy.
    q = Mock(spec=Query)
    q.md5 = "dummy_query_id"

    mock_func_cache_table_exists = Mock()
    monkeypatch.setattr(
        "flowmachine.core.server.query_proxy.cache_table_exists",
        mock_func_cache_table_exists,
    )

    def dummy_construct_query_object(query_kind, params):
        return q

    #
    # Construct query proxy
    #
    query_proxy = QueryProxy(
        "dummy_query",
        {"param": "some_value"},
        redis=dummy_redis,
        func_construct_query_object=dummy_construct_query_object,
    )

    #
    # Run the query and get the query id
    #
    query_id = query_proxy.run_query_async()

    #
    # Poll the query a few times and check the return status is as expectd
    # based on whether a redis lock and/or the cache table exists.
    #
    qsm = QueryStateMachine(dummy_redis, query_id=q.md5)
    dummy_redis._store[qsm.state_machine._name] = QueryState.EXECUTING.value.encode()
    assert QueryState.EXECUTING.value == query_proxy.poll()

    dummy_redis._store[qsm.state_machine._name] = QueryState.EXECUTED.value.encode()
    mock_func_cache_table_exists.return_value = False
    assert "awol" == query_proxy.poll()
    mock_func_cache_table_exists.return_value = True
    assert "executed" == query_proxy.poll()


def test_get_sql(dummy_redis, monkeypatch):
    """
    Running get_sql returns the expected sql.
    """
    # Define mock query object and a function which returns it when called.
    # This serves as a drop-in replacement for 'construct_query_object' in
    # flowmachine.core.server.query_proxy.
    q = Mock(spec=Query)
    q.md5 = "dummy_query_id"

    def dummy_construct_query_object(query_kind, params):
        return q

    #
    # Construct query proxy
    #
    query_proxy = QueryProxy(
        "dummy_query",
        {"param": "some_value"},
        redis=dummy_redis,
        func_construct_query_object=dummy_construct_query_object,
    )

    # Set conditions for this test
    monkeypatch.setattr(
        "flowmachine.core.server.query_proxy.cache_table_exists",
        lambda connection, query_id: True,
    )
    # Advance query state to executed
    qsm = QueryStateMachine(dummy_redis, query_id=q.md5)
    qsm.enqueue()
    qsm.execute()
    qsm.finish()
    monkeypatch.setattr(
        "flowmachine.core.server.query_proxy.get_sql_for_query_id",
        lambda query_id: "SELECT * FROM dummy_table",
    )

    #
    # Run the query and get the query id
    #
    query_proxy.run_query_async()
    q.store.assert_called_once_with()
    assert "executed" == query_proxy.poll()

    #
    # Get SQL code and check it is as expected
    #
    sql = query_proxy.get_sql()
    assert "SELECT * FROM dummy_table" == sql


@pytest.mark.parametrize(
    "current_state, expected_error",
    [
        (QueryState.KNOWN, MissingQueryError),
        (QueryState.QUEUED, QueryProxyError),
        (QueryState.CANCELLED, QueryProxyError),
        (QueryState.RESETTING, QueryProxyError),
        (QueryState.EXECUTING, QueryProxyError),
        (Mock(), QueryProxyError),
    ],
)
def test_get_sql_errors(current_state, expected_error, dummy_redis, monkeypatch):
    """
    Running get_sql raises expected exceptions.
    """
    # Define mock query object and a function which returns it when called.
    # This serves as a drop-in replacement for 'construct_query_object' in
    # flowmachine.core.server.query_proxy.
    q = Mock(spec=Query)
    q.md5 = "dummy_query_id"

    def dummy_construct_query_object(query_kind, params):
        return q

    #
    # Construct query proxy
    #
    query_proxy = QueryProxy(
        "dummy_query",
        {"param": "some_value"},
        redis=dummy_redis,
        func_construct_query_object=dummy_construct_query_object,
    )
    #
    # Run the query and get the query id
    #
    query_id = query_proxy.run_query_async()

    # Set conditions for this test
    monkeypatch.setattr(
        "flowmachine.core.server.query_proxy.cache_table_exists",
        lambda connection, query_id: True,
    )
    # Set query state
    qsm = QueryStateMachine(dummy_redis, query_id=q.md5)
    dummy_redis._store[qsm.state_machine._name] = current_state.value.encode()
    with pytest.raises(expected_error):
        sql = query_proxy.get_sql()
