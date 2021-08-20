# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pdb
from asyncio import sleep

import pytest
from marshmallow import Schema, fields

import flowmachine
from flowmachine import connections
from flowmachine.core.cache import get_query_object_by_id, reset_cache
from flowmachine.core.context import (
    get_redis,
    get_db,
    redis_connection,
    context,
    get_executor,
)
from flowmachine.core.query_info_lookup import QueryInfoLookup
from flowmachine.core.query_state import QueryState, QueryStateMachine

from flowmachine.core.server.action_handlers import (
    action_handler__get_geography,
    action_handler__get_query_params,
    action_handler__get_sql,
    action_handler__run_query,
    action_handler__bench_query,
    action_handler__poll_query,
    get_action_handler,
    _load_query_from_params,
)
from flowmachine.core.server.exceptions import FlowmachineServerError
from flowmachine.core.server.query_schemas import FlowmachineQuerySchema
from flowmachine.core.server.zmq_helpers import ZMQReplyStatus


def test_bad_action_handler():
    """Exception should be raised if we try to get a handler that doesn't exist."""
    with pytest.raises(FlowmachineServerError):
        get_action_handler("NOT_A_HANDLER")


@pytest.mark.asyncio
async def test_rerun_query_after_removed_from_cache(
    dummy_redis, server_config, real_connections
):
    """
    Test that a query can be rerun after it has been removed from the cache.
    """
    msg = await action_handler__run_query(
        config=server_config,
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    query_id = msg["payload"]["query_id"]
    qsm = QueryStateMachine(get_redis(), query_id, get_db().conn_id)
    qsm.wait_until_complete()
    query_obj = get_query_object_by_id(get_db(), query_id)
    assert query_obj.is_stored
    query_obj.invalidate_db_cache()
    assert not query_obj.is_stored
    assert qsm.is_known
    msg = await action_handler__run_query(
        config=server_config,
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    assert msg["status"] == ZMQReplyStatus.SUCCESS
    qsm.wait_until_complete()
    assert query_obj.is_stored


@pytest.fixture
def real_connections(flowmachine_connect):
    with connections():
        try:
            yield
        finally:
            reset_cache(get_db(), get_redis(), protect_table_objects=False)
            get_db().engine.dispose()  # Close the connection
            get_redis().flushdb()  # Empty the redis


@pytest.mark.asyncio
async def test_rerun_query_after_cancelled(server_config, real_connections):
    """
    Test that a query can be rerun after it has been cancelled.
    """
    query_obj = (
        FlowmachineQuerySchema()
        .load(
            dict(
                query_kind="spatial_aggregate",
                locations=dict(
                    query_kind="daily_location",
                    date="2016-01-01",
                    method="last",
                    aggregation_unit="admin3",
                ),
            )
        )
        ._flowmachine_query_obj
    )
    query_id = query_obj.query_id
    qsm = QueryStateMachine(get_redis(), query_id, get_db().conn_id)
    qsm.enqueue()
    qsm.cancel()
    assert not query_obj.is_stored
    assert qsm.is_cancelled
    query_info_lookup = QueryInfoLookup(get_redis())
    query_info_lookup.register_query(
        query_id,
        dict(
            query_kind="spatial_aggregate",
            locations=dict(
                query_kind="daily_location",
                date="2016-01-01",
                method="last",
                aggregation_unit="admin3",
            ),
        ),
    )

    msg = await action_handler__run_query(
        config=server_config,
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    assert msg["status"] == ZMQReplyStatus.SUCCESS
    qsm.wait_until_complete()
    assert query_obj.is_stored


@pytest.mark.asyncio
async def test_run_query_type_error(monkeypatch, server_config):
    """
    Test that developer errors when creating schemas return the error message.
    """

    class BrokenSchema(Schema):
        def load(self, *args, **kwargs):
            raise TypeError("DUMMY_FAIL_MESSAGE")

    monkeypatch.setattr(
        flowmachine.core.server.action_handlers, "FlowmachineQuerySchema", BrokenSchema
    )
    msg = await action_handler__run_query(config=server_config, query_kind={})
    assert msg.status == ZMQReplyStatus.ERROR
    assert (
        msg.msg
        == "Internal flowmachine server error: could not create query object using query schema. The original error was: 'DUMMY_FAIL_MESSAGE'"
    )


@pytest.mark.asyncio
async def test_run_query_error_handled(dummy_redis, server_config):
    """
    Run query handler should return an error status if query construction failed.
    """
    # This is going to error, because the db connection is a mock.
    msg = await action_handler__run_query(
        config=server_config,
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    assert msg.status == ZMQReplyStatus.ERROR
    assert (
        msg.msg
        == "Internal flowmachine server error: could not create query object using query schema. The original error was: 'type object argument after * must be an iterable, not Mock'"
    )


@pytest.mark.parametrize("bad_unit", ["NOT_A_VALID_UNIT", "admin4"])
@pytest.mark.asyncio
async def test_geo_handler_bad_unit(bad_unit, server_config):
    """
    Geo handler should send back a message with error status for bad, or missing units
    """
    msg = await action_handler__get_geography(
        config=server_config, aggregation_unit=bad_unit
    )
    assert msg.status == ZMQReplyStatus.ERROR


@pytest.mark.asyncio
async def test_get_query_bad_id(server_config):
    """
    Get sql handler should send back an error status for a nonexistent id
    """
    get_redis().get.return_value = None
    msg = await action_handler__get_query_params(
        config=server_config, query_id="DUMMY_ID"
    )
    assert msg.status == ZMQReplyStatus.ERROR


@pytest.mark.parametrize(
    "query_state", [state for state in QueryState if state is not QueryState.COMPLETED]
)
@pytest.mark.asyncio
async def test_get_sql_error_states(query_state, dummy_redis, server_config):
    """
    Test that get_sql handler replies with an error state when the query
    is not finished.
    """
    redis_reset = redis_connection.set(dummy_redis)
    dummy_redis.set("DUMMY_QUERY_ID", "KNOWN")
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID", get_db().conn_id)
    dummy_redis.set(state_machine.state_machine._name, query_state)
    msg = await action_handler__get_sql(config=server_config, query_id="DUMMY_QUERY_ID")
    assert msg.status == ZMQReplyStatus.ERROR
    assert msg.payload["query_state"] == query_state
    redis_connection.reset(redis_reset)


@pytest.mark.asyncio
async def test_action_handler__bench_query(server_config, real_connections):
    action_params = dict(
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )

    reply = await action_handler__bench_query(server_config, **action_params)
    query_id = reply.payload["query_id"]
    qsm = QueryStateMachine(get_redis(), query_id, get_db().conn_id)
    while qsm.current_query_state == QueryState.EXECUTING:
        pass
    assert qsm.current_query_state == QueryState.COMPLETED
    result = await action_handler__poll_query(server_config, query_id)
    assert result.payload["query_state"] == "completed"
    params = await action_handler__get_query_params(server_config, query_id)
    out_sql = await action_handler__get_sql(server_config, query_id)
    # Under the standard situation, the benchmark query should not run against cache
    assert out_sql
    assert "CACHE" not in out_sql.payload["sql"]
    pass


def test_load_query_from_params():
    action_params = dict(
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    assert type(_load_query_from_params(action_params)) == "ExposedSpatialAggregate"
    bench_params = dict(query_kind="benchmark", benchmark_target=action_params)
    assert type(_load_query_from_params(bench_params)) == "ExposedBenchmark"
