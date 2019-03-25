# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from json import loads

from flowapi.zmq_helpers import ZMQReply


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
        "status": "done",
        "payload": {
            "query_id": "5ffe4a96dbe33a117ae9550178b81836",
            "query_kind": "modal_location",
            "query_state": "completed",
        },
    }
    reply_2 = {
        "status": "done",
        "payload": {"query_params": {"aggregation_unit": "DUMMY_AGGREGATION"}},
    }
    reply_3 = {
        "status": "done",
        "payload": {"query_state": "completed", "sql": "SELECT 1;"},
    }
    dummy_zmq_server.side_effect = (reply_1, reply_2, reply_3)
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
        (
            "done",
            "completed",
            200,
        ),  # disabling this because it is tested in `test_get_query` above and the return message from flowmachine now has a different structure to the other cases
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
            status="done",
            payload={"query_id": "DUMMY_QUERY_ID", "query_kind": "modal_location"},
        ).as_json(),
        ZMQReply(
            status="done",
            payload={
                "query_id": "DUMMY_QUERY_ID",
                "query_params": {"aggregation_unit": "DUMMY_AGGREGATION"},
            },
        ).as_json(),
        ZMQReply(
            status=reply_msg_status,
            msg="Some error",  # note: in a real zmq message this would only be present in the "error" case, but we provide it for all test cases (it is simply ignored in the success case)
            payload={
                "query_id": "DUMMY_QUERY_ID",
                "query_state": query_state,
                "sql": "SELECT 1;",  # note: in a real zmq message this would only be present in the "success" case, but we provide it for all test cases (it is simply ignored in the error case)
            },
        ).as_json(),
    )
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert http_code == response.status_code


@pytest.mark.asyncio
async def test_get_error_for_missing_status(
    app, dummy_zmq_server, access_token_builder
):
    """
    Test that status code 500 is returned if a message without a status is
    received in get_query.
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
    dummy_zmq_server.side_effect = (
        {"id": 0, "query_kind": "modal_location"},
        {"id": 0, "params": {"aggregation_unit": "DUMMY_AGGREGATION"}},
        {"id": 0},
    )
    response = await client.get(
        f"/api/0/get/0", headers={"Authorization": f"Bearer {token}"}
    )
    assert 500 == response.status_code
