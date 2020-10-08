# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from json import loads

from tests.unit.zmq_helpers import ZMQReply

import pytest
from asynctest import CoroutineMock


@pytest.mark.parametrize(
    "filetype, expected, expected_file_ext",
    [
        (
            ".json",
            b'{"query_id":"DUMMY_QUERY_ID", "query_result":[{"key":"value1"}, {"key":"value2"}]}',
            "json",
        ),
        (
            "",
            b'{"query_id":"DUMMY_QUERY_ID", "query_result":[{"key":"value1"}, {"key":"value2"}]}',
            "json",
        ),
        (
            ".geojson",
            b'{"type":"FeatureCollection", "features":[{"key":"value1"}, {"key":"value2"}]}',
            "geojson",
        ),
        (
            ".csv",
            b"key\r\nvalue1\r\nvalue2\r\n",
            "csv",
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_query(
    filetype, expected, expected_file_ext, app, access_token_builder, dummy_zmq_server
):
    """
    Test that JSON is returned when getting a query.
    """

    # Set the rows returned by iterating over the rows from the db
    # This is a long chain of mocks corresponding to getting a connection using
    # the pool's context manager, getting the cursor on that, and then looping
    # over the values in cursor
    app.db_pool.acquire.return_value.__aenter__.return_value.cursor.return_value.__aiter__.return_value = [
        {"key": "value1"},
        {"key": "value2"},
    ]
    token = access_token_builder(
        [
            "get_result&modal_location.aggregation_unit.DUMMY_AGGREGATION",
            "get_result&geography.aggregation_unit.DUMMY_AGGREGATION",
        ]
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
        "payload": {
            "query_state": "completed",
            "sql": "SELECT 1;",
            "aggregation_unit": "DUMMY_AGGREGATION",
        },
    }
    dummy_zmq_server.side_effect = (reply_1, reply_2)
    response = await app.client.get(
        f"/api/0/get/DUMMY_QUERY_ID{filetype}",
        headers={"Authorization": f"Bearer {token}"},
    )
    reply = await response.get_data()
    assert expected == reply
    assert (
        f"attachment;filename=DUMMY_QUERY_ID.{expected_file_ext}"
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

    token = access_token_builder(
        ["get_result&modal_location.aggregation_unit.DUMMY_AGGREGATION"]
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
    response = await app.client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert http_code == response.status_code


@pytest.mark.asyncio
async def test_get_error_message_without_query_state(
    app, access_token_builder, dummy_zmq_server, monkeypatch
):
    """
    Test that the get/ endpoint returns the correct error message if the reply
    payload does not contain a query state.
    """

    # Monkeypatch can_get_results_by_query_id so that we don't have to set a sequence
    # of replies as side-effects of dummy_zmq_server just to verify token permissions
    monkeypatch.setattr(
        "flowapi.user_model.UserObject.can_get_results_by_query_id",
        CoroutineMock(return_value=True),
    )
    dummy_zmq_server.return_value = ZMQReply(status="error", msg="DUMMY_ERROR_MESSAGE")
    response = await app.client.get(
        f"/api/0/get/DUMMY_QUERY_ID",
        headers={"Authorization": f"Bearer {access_token_builder({})}"},
    )
    json = await response.get_json()
    assert 500 == response.status_code
    assert "DUMMY_ERROR_MESSAGE" == json["msg"]
