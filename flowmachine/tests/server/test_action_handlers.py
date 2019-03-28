# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from flowmachine.core import Query
from flowmachine.core.query_state import QueryState, QueryStateMachine

from flowmachine.core.server.action_handlers import (
    action_handler__get_geography,
    action_handler__get_query_params,
    action_handler__get_sql,
)
from flowmachine.core.server.zmq_helpers import ZMQReplyStatus


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
def test_get_sql_error_states(query_state, dummy_redis):
    """
    Test that get_sql handler replies with an error state when the query
    is not finished.
    """
    Query.redis = dummy_redis
    dummy_redis.set("DUMMY_QUERY_ID", "KNOWN")
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID")
    dummy_redis.set(state_machine.state_machine._name, query_state)
    msg = action_handler__get_sql("DUMMY_QUERY_ID")
    assert msg.status == ZMQReplyStatus.ERROR
    assert msg.payload["query_state"] == query_state
