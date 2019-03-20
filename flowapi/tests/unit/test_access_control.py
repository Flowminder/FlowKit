# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import os

import pytest
from asynctest import return_once
from .utils import query_kinds, exemplar_query_params


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["/api/0/poll/foo", "/api/0/get/foo"])
async def test_protected_get_routes(route, app):
    """
    Test that protected routes return a 401 without a valid token.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    route: str
        Route to test
    """
    client, db, log_dir, app = app

    response = await client.get(route)
    assert 401 == response.status_code
    with open(os.path.join(log_dir, "flowkit-access.log")) as log_file:
        log_lines = log_file.readlines()
    assert 1 == len(log_lines)
    assert "UNAUTHORISED" in log_lines[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_granular_run_access(
    query_kind, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to running queries.

    """
    client, db, log_dir, app = app
    token = access_token_builder({query_kind: {"permissions": {"run": True}}})
    expected_responses = dict.fromkeys(query_kinds, 401)
    expected_responses[query_kind] = 202
    dummy_zmq_server.return_value = {
        "status": "accepted",
        "msg": "",
        "payload": {"query_id": "DUMMY_QUERY_ID"},
    }
    responses = {}
    for q_kind in query_kinds:
        q_params = exemplar_query_params[q_kind]
        response = await client.post(
            f"/api/0/run", headers={"Authorization": f"Bearer {token}"}, json=q_params
        )
        responses[q_kind] = response.status_code
    assert expected_responses == responses


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_granular_poll_access(
    query_kind, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to checking query status.

    """
    client, db, log_dir, app = app
    token = access_token_builder({query_kind: {"permissions": {"poll": True}}})
    expected_responses = dict.fromkeys(query_kinds, 401)
    expected_responses[query_kind] = 303

    responses = {}
    for q_kind in query_kinds:
        dummy_zmq_server.side_effect = return_once(
            {
                "status": "done",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_kind": q_kind,
                    "query_state": "executing",
                },
            },
            then={
                "status": "done",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_kind": q_kind,
                    "query_state": "completed",
                },
            },
        )
        response = await client.get(
            f"/api/0/poll/DUMMY_QUERY_ID",
            headers={"Authorization": f"Bearer {token}"},
            json={"query_kind": q_kind},
        )
        responses[q_kind] = response.status_code
    assert expected_responses == responses


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_granular_json_access(
    query_kind, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to query output.

    """
    client, db, log_dir, app = app
    token = access_token_builder(
        {
            query_kind: {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )
    expected_responses = dict.fromkeys(query_kinds, 401)
    expected_responses[query_kind] = 200
    responses = {}
    for q_kind in query_kinds:
        dummy_zmq_server.side_effect = (
            {
                "status": "done",
                "msg": "",
                "payload": {"query_id": "DUMMY_QUERY_ID", "query_kind": q_kind},
            },
            {
                "status": "done",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_params": {"aggregation_unit": "DUMMY_AGGREGATION"},
                },
            },
            {
                "status": "done",
                "msg": "",
                "payload": {"query_id": "DUMMY_QUERY_ID", "sql": "SELECT 1;"},
            },
        )
        response = await client.get(
            f"/api/0/get/DUMMY_QUERY_ID",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )
        responses[q_kind] = response.status_code
    assert expected_responses == responses


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "claims",
    [
        {"permissions": {"get_result": True}, "spatial_aggregation": []},
        {"permissions": {}, "spatial_aggregation": ["DUMMY_AGGREGATION"]},
    ],
)
async def test_no_result_access_without_both_claims(
    claims, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to query output.

    """
    client, db, log_dir, app = app
    token = access_token_builder({"DUMMY_QUERY_KIND": claims})
    dummy_zmq_server.side_effect = (
        {
            "status": "done",
            "msg": "",
            "payload": {"query_id": "DUMMY_QUERY_ID", "query_kind": "dummy_query"},
        },
        {
            "status": "done",
            "msg": "",
            "payload": {
                "query_id": "DUMMY_QUERY_ID",
                "query_params": {"aggregation_unit": "DUMMY_AGGREGATION"},
            },
        },
        {
            "status": "done",
            "msg": "",
            "payload": {"query_id": "DUMMY_QUERY_ID", "sql": "SELECT 1;"},
        },
    )
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID",
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": "DUMMY_QUERY_KIND"},
    )
    assert 401 == response.status_code


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
@pytest.mark.parametrize(
    "route", ["/api/0/poll/DUMMY_QUERY_ID", "/api/0/get/DUMMY_QUERY_ID"]
)
async def test_access_logs_gets(
    query_kind, route, app, access_token_builder, dummy_zmq_server
):
    """
    Test that access logs are written for attempted unauthorized access to 'poll' and get' routes.

    """
    client, db, log_dir, app = app
    token = access_token_builder({query_kind: {"permissions": {}}})
    dummy_zmq_server.return_value = {
        "status": "done",
        "msg": "",
        "payload": {"query_id": "DUMMY_QUERY_ID", "query_kind": "dummy_query_kind"},
    }
    response = await client.get(
        route,
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": query_kind},
    )
    assert 401 == response.status_code
    with open(os.path.join(log_dir, "query-runs.log")) as log_file:
        log_lines = log_file.readlines()
    assert 2 == len(log_lines)
    assert "DUMMY_QUERY_KIND" == json.loads(log_lines[0])["query_kind"]
    assert "CLAIM_TYPE_NOT_ALLOWED_BY_TOKEN" in log_lines[1]
    assert "test" in log_lines[0]
    assert "test" in log_lines[1]
    assert (
        json.loads(log_lines[0])["request_id"] == json.loads(log_lines[1])["request_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_access_logs_post(query_kind, app, access_token_builder):
    """
    Test that access logs are written for attempted unauthorized access to 'run' route.

    """
    client, db, log_dir, app = app
    token = access_token_builder({query_kind: {"permissions": {}}})
    response = await client.post(
        f"/api/0/run",
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": query_kind},
    )
    assert 401 == response.status_code
    with open(os.path.join(log_dir, "query-runs.log")) as log_file:
        log_lines = log_file.readlines()
    assert 2 == len(log_lines)
    assert query_kind.upper() in log_lines[0]
    assert "CLAIM_TYPE_NOT_ALLOWED_BY_TOKEN" in log_lines[1]
    assert "test" in log_lines[0]
    assert "test" in log_lines[1]
