# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from marshmallow import Schema, fields

import flowmachine
from flowmachine.core import Query
from flowmachine.core.query_state import QueryState, QueryStateMachine

from flowmachine.core.server.action_handlers import (
    action_handler__get_geography,
    action_handler__get_query_params,
    action_handler__get_sql,
    action_handler__run_query,
    get_action_handler,
)
from flowmachine.core.server.exceptions import FlowmachineServerError
from flowmachine.core.server.zmq_helpers import ZMQReplyStatus


def test_bad_action_handler():
    """Exception should be raised if we try to get a handler that doesn't exist."""
    with pytest.raises(FlowmachineServerError):
        get_action_handler("NOT_A_HANDLER")


def test_run_query_type_error(monkeypatch):
    """
    Test that developer errors when creating schemas return the error message.
    """

    class BrokenSchema(Schema):
        def load(self, *args, **kwargs):
            raise TypeError("DUMMY_FAIL_MESSAGE")

    monkeypatch.setattr(
        flowmachine.core.server.action_handlers, "FlowmachineQuerySchema", BrokenSchema
    )
    msg = action_handler__run_query(query_kind={})
    assert msg.status == ZMQReplyStatus.ERROR
    assert (
        msg.msg
        == "Internal flowmachine server error: could not create query object using query schema. The original error was: 'DUMMY_FAIL_MESSAGE'"
    )


def test_run_query_error_handled(monkeypatch, dummy_redis):
    """
    Run query handler should return an error status if query construction failed.
    """
    monkeypatch.setattr(Query, "redis", dummy_redis)
    msg = action_handler__run_query(
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-02-02",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    assert msg.status == ZMQReplyStatus.ERROR
    assert msg.msg == "Unable to create query object."


@pytest.mark.parametrize("bad_unit", ["NOT_A_VALID_UNIT", "admin4"])
def test_geo_handler_bad_unit(bad_unit):
    """
    Geo handler should send back a message with error status for bad, or missing units
    """
    msg = action_handler__get_geography(bad_unit)
    assert msg.status == ZMQReplyStatus.ERROR


def test_get_query_bad_id():
    """
    Get sql handler should send back an error status for a nonexistent id
    """
    Query.redis.get.return_value = None
    msg = action_handler__get_query_params("DUMMY_ID")
    assert msg.status == ZMQReplyStatus.ERROR


@pytest.mark.parametrize(
    "query_state", [state for state in QueryState if state is not QueryState.COMPLETED]
)
def test_get_sql_error_states(query_state, dummy_redis, monkeypatch):
    """
    Test that get_sql handler replies with an error state when the query
    is not finished.
    """
    monkeypatch.setattr(Query, "redis", dummy_redis)
    dummy_redis.set("DUMMY_QUERY_ID", "KNOWN")
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID")
    dummy_redis.set(state_machine.state_machine._name, query_state)
    msg = action_handler__get_sql("DUMMY_QUERY_ID")
    assert msg.status == ZMQReplyStatus.ERROR
    assert msg.payload["query_state"] == query_state
