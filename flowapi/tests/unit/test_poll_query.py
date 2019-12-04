# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from asynctest import return_once

from tests.unit.zmq_helpers import ZMQReply


@pytest.mark.asyncio
async def test_poll_bad_query(app, access_token_builder, dummy_zmq_server):
    """
    Test that correct status code and any redirect is returned when polling a running query
    """

    token = token = access_token_builder(
        [f"run:modal_location:aggregation_unit:DUMMY_AGGREGATION"]
    )

    dummy_zmq_server.side_effect = return_once(
        ZMQReply(
            status="error",
            msg=f"Unknown query id: 'DUMMY_QUERY_ID'",
            payload={"query_id": "DUMMY_QUERY_ID", "query_state": "awol"},
        )
    )
    response = await app.client.get(
        f"/api/0/poll/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 404


@pytest.mark.parametrize("action_right", ("get_result", "run"))
@pytest.mark.parametrize(
    "query_state, http_code",
    [
        ("completed", 303),
        ("executing", 202),
        ("awol", 404),
        ("queued", 202),
        ("errored", 500),
        ("cancelled", 500),
    ],
)
@pytest.mark.asyncio
async def test_poll_query(
    action_right, query_state, http_code, app, access_token_builder, dummy_zmq_server
):
    """
    Test that correct status code and any redirect is returned when polling a running query
    """

    token = access_token_builder(
        [f"{action_right}:modal_location:aggregation_unit:DUMMY_AGGREGATION"]
    )

    # The replies below are in response to the following messages:
    #  - get_query_kind
    #  - poll_query
    #
    # {'status': 'done', 'msg': '', 'payload': {'query_id': '5ffe4a96dbe33a117ae9550178b81836', 'query_kind': 'modal_location'}}
    # {'status': 'done', 'msg': '', 'payload': {'query_id': '5ffe4a96dbe33a117ae9550178b81836', 'query_kind': 'modal_location', 'query_state': 'completed'}}
    #
    dummy_zmq_server.side_effect = return_once(
        ZMQReply(
            status="success",
            payload={
                "query_id": "DUMMY_QUERY_ID",
                "query_params": {
                    "query_kind": "modal_location",
                    "aggregation_unit": "DUMMY_AGGREGATION",
                },
            },
        ),
        then=ZMQReply(
            status="success",
            payload={"query_id": "DUMMY_QUERY_ID", "query_state": query_state},
        ),
    )
    response = await app.client.get(
        f"/api/0/poll/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == http_code
    if query_state == "success":
        assert "/api/0/get/DUMMY_QUERY_ID" == response.headers["Location"]


@pytest.mark.asyncio
async def test_poll_query_query_error(app, access_token_builder, dummy_zmq_server):
    """
    Test that correct status code and any redirect is returned when polling a query that errored
    """

    token = access_token_builder({"modal_location": {"permissions": {"poll": True}}})

    # TODO: Fix the logic that makes this necessary
    dummy_zmq_server.side_effect = return_once(
        ZMQReply(
            status="success",
            payload={"query_id": "DUMMY_QUERY_ID", "query_kind": "modal_location"},
        ),
        then=ZMQReply(
            status="error",
            payload={"query_id": "DUMMY_QUERY_ID", "query_state": "error"},
        ),
    )
    response = await app.client.get(
        f"/api/0/poll/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 500
