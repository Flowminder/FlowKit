import pytest

from flowmachine.core.query_info_lookup import QueryInfoLookup, QueryInfoLookupError


def test_register_query(dummy_redis):
    """
    Test that a query is known after registering it.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)
    assert not q_info_lookup.query_is_known("dummy_id")
    q_info_lookup.register_query(
        query_id="dummy_id",
        query_params={"query_kind": "dummy_query", "dummy_param": "some_value"},
    )
    assert q_info_lookup.query_is_known("dummy_id")


def test_cannot_register_query_params_without_query_kind(dummy_redis):
    """
    Test that registering a query fails if the parameters do not contain the 'query_kind' key.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)

    with pytest.raises(
        QueryInfoLookupError, match="Query params must contain a 'query_kind' entry."
    ):
        q_info_lookup.register_query(
            query_id="dummy_id", query_params={"dummy_param": "some_value"}
        )


def test_retrieve_query_params_fails_for_unknown_query(dummy_redis):
    """
    Test that retrieving a query's parameters raises an error if it has not been registered.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)

    with pytest.raises(QueryInfoLookupError, match="Unknown query_id: 'dummy_id'"):
        q_info_lookup.get_query_params("dummy_id")


def test_retrieve_query_kind_fails_for_unknown_query(dummy_redis):
    """
    Test that retrieving a query's parameters raises an error if it has not been registered.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)

    with pytest.raises(QueryInfoLookupError, match="Unknown query_id: 'dummy_id'"):
        q_info_lookup.get_query_kind("dummy_id")


def test_retrieve_query_params(dummy_redis):
    """
    Test that we can retrieve a query's parameters after registering it.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)
    q_info_lookup.register_query(
        query_id="dummy_id",
        query_params={"query_kind": "dummy_query", "dummy_param": "some_value"},
    )

    expected_query_params = {"query_kind": "dummy_query", "dummy_param": "some_value"}
    assert expected_query_params == q_info_lookup.get_query_params("dummy_id")


def test_retrieve_query_kind(dummy_redis):
    """
    Test that we can retrieve the query kind after registering a query.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)
    q_info_lookup.register_query(
        query_id="dummy_id",
        query_params={"query_kind": "dummy_query", "dummy_param": "some_value"},
    )

    assert "dummy_query" == q_info_lookup.get_query_kind("dummy_id")


def test_retrieve_query_parameters(dummy_redis):
    """
    Test that we can retrieve the query parameters after registering a query.
    """
    q_info_lookup = QueryInfoLookup(dummy_redis)
    q_info_lookup.register_query(
        query_id="dummy_id",
        query_params={"query_kind": "dummy_query", "dummy_param": "some_value"},
    )

    expected_query_params = {"query_kind": "dummy_query", "dummy_param": "some_value"}
    assert expected_query_params == q_info_lookup.get_query_params("dummy_id")
