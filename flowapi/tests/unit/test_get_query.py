# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from json import loads

from tests.unit.zmq_helpers import ZMQReply


@pytest.mark.asyncio
async def test_get_query(app, access_token_builder, dummy_zmq_server):
    """
    Test that JSON is returned when getting a query.
    """
    client, db, log_dir, app = app
    # Set the rows returned by iterating over the rows from the db
    # This is a long chain of mocks corresponding to getting a connection using
    # the pool's context manager, getting the cursor on that, and then looping
    # over the values in cursor
    db.acquire.return_value.__aenter__.return_value.cursor.return_value.__aiter__.return_value = [
        {"some": "valid"},
        {"json": "bits"},
    ]
    token = access_token_builder(
        {
            "modal_location": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )

    reply_1 = {
        "status": "success",
        "payload": {
            "query_id": "5ffe4a96dbe33a117ae9550178b81836",
            "query_params": {
                "aggregation_unit": "DUMMY_AGGREGATION",
                "query_kind": "modal_location",
            },
        },
    }

    reply_2 = {
        "status": "success",
        "payload": {"query_state": "completed", "sql": "SELECT 1;"},
    }
    dummy_zmq_server.side_effect = (reply_1, reply_2)
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    reply = await response.get_data()
    json_data = loads(reply)
    assert "DUMMY_QUERY_ID" == json_data["query_id"]
    assert [{"some": "valid"}, {"json": "bits"}] == json_data["query_result"]
    assert (
        "attachment;filename=DUMMY_QUERY_ID.json"
        == response.headers["content-disposition"]
    )


# FIXME: this test is very difficult to adjust and debug when things change
# on the flowmachine side (e.g. in the structure of the zmq reply message).
# It should probably be turned into an integration test, or we should rethink
# how/what we are testing here.
@pytest.mark.parametrize(
    "reply_msg_status, query_state, http_code",
    [
        ("success", "completed", 200),
        ("error", "executing", 202),
        ("error", "queued", 202),
        ("error", "awol", 404),
        ("error", "errored", 403),
        ("error", "known", 404),
        ("error", "NOT_A_STATUS", 500),
    ],
)
@pytest.mark.asyncio
async def test_get_json_status_code(
    reply_msg_status,
    query_state,
    http_code,
    app,
    access_token_builder,
    dummy_zmq_server,
):
    """
    Test that correct status code and any redirect is returned when getting json.
    """
    client, db, log_dir, app = app

    token = access_token_builder(
        {
            "modal_location": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )

    # The replies below are in response to the following messages:
    #  - get_query_kind
    #  - get_query_params
    #  - get_sql_for_query_result
    dummy_zmq_server.side_effect = (
        ZMQReply(
            status="success",
            payload={
                "query_id": "DUMMY_QUERY_ID",
                "query_params": {
                    "aggregation_unit": "DUMMY_AGGREGATION",
                    "query_kind": "modal_location",
                },
            },
        ),
        ZMQReply(
            status=reply_msg_status,
            msg="Some error",  # note: in a real zmq message this would only be present in the "error" case, but we provide it for all test cases (it is simply ignored in the success case)
            payload={
                "query_id": "DUMMY_QUERY_ID",
                "query_state": query_state,
                "sql": "SELECT 1;",  # note: in a real zmq message this would only be present in the "success" case, but we provide it for all test cases (it is simply ignored in the error case)
            },
        ),
    )
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert http_code == response.status_code


@pytest.mark.asyncio
async def test_get_error_message_without_query_state(
    app, access_token_builder, dummy_zmq_server, async_return, monkeypatch
):
    """
    Test that the get/ endpoint returns the correct error message if the reply
    payload does not contain a query state.
    """
    client, db, log_dir, app = app

    # Monkeypatch can_get_results_by_query_id so that we don't have to set a sequence
    # of replies as side-effects of dummy_zmq_server just to verify token permissions
    monkeypatch.setattr(
        "flowapi.user_model.UserObject.can_get_results_by_query_id",
        lambda self, query_id: async_return(True),
    )
    dummy_zmq_server.return_value = ZMQReply(status="error", msg="DUMMY_ERROR_MESSAGE")
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID",
        headers={"Authorization": f"Bearer {access_token_builder({})}"},
    )
    json = await response.get_json()
    assert 500 == response.status_code
    assert "DUMMY_ERROR_MESSAGE" == json["msg"]
