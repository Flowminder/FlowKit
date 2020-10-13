# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest

from .utils import exemplar_query_params, query_kinds


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["/api/0/poll/foo", "/api/0/get/foo"])
async def test_protected_get_routes(route, app):
    """
    Test that protected routes return a 401 without a valid token.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi app
    route: str
        Route to test
    """

    response = await app.client.get(route)
    assert 401 == response.status_code

    log_lines = app.log_capture().access
    assert 1 == len(log_lines)  # One entry written to stdout

    assert "UNAUTHORISED" == log_lines[0]["event"]


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_granular_run_access(
    query_kind, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to running queries.

    """

    token = access_token_builder([f"run&{exemplar_query_params[query_kind]['token']}"])
    expected_responses = dict.fromkeys(query_kinds, 403)
    expected_responses[query_kind] = 202
    dummy_zmq_server.return_value = {
        "status": "success",
        "msg": "",
        "payload": {
            "query_id": "DUMMY_QUERY_ID",
            "progress": {"eligible": 0, "queued": 0, "executing": 0},
        },
    }
    responses = {}
    for q_kind in query_kinds:
        q_params = exemplar_query_params[q_kind]["params"]
        response = await app.client.post(
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

    token = access_token_builder([f"run&{exemplar_query_params[query_kind]['token']}"])

    expected_responses = dict.fromkeys(query_kinds, 403)
    expected_responses[query_kind] = 303

    responses = {}
    for q_kind in query_kinds:
        dummy_zmq_server.side_effect = (
            {
                "status": "success",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_params": exemplar_query_params[q_kind]["params"],
                },
            },
            {
                "status": "success",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_kind": q_kind,
                    "query_state": "completed",
                },
            },
        )
        response = await app.client.get(
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

    token = access_token_builder(
        [f"get_result&{exemplar_query_params[query_kind]['token']}"]
    )

    expected_responses = dict.fromkeys(query_kinds, 403)
    expected_responses[query_kind] = 200
    responses = {}
    for q_kind in query_kinds:
        dummy_zmq_server.side_effect = (
            {
                "status": "success",
                "msg": "",
                "payload": {
                    "query_id": "DUMMY_QUERY_ID",
                    "query_params": exemplar_query_params[q_kind]["params"],
                },
            },
            {
                "status": "success",
                "msg": "",
                "payload": {"query_id": "DUMMY_QUERY_ID", "sql": "SELECT 1;"},
            },
        )
        response = await app.client.get(
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
        "get_result&{query_kind}",
        "{query_kind}:aggregation_unit.DUMMY_AGGREGATION",
        "get_result&{query_kind}:aggregation_unit.A_DIFFERENT_AGGREGATION",
        "run&{query_kind}:aggregation_unit.DUMMY_AGGREGATION",
    ],
)
async def test_no_result_access_without_both_claims(
    claims, app, access_token_builder, dummy_zmq_server
):
    """
    Test that tokens grant granular access to query output.

    """
    token = access_token_builder([claims.format(query_kind="DUMMY_QUERY_KIND")])
    dummy_zmq_server.side_effect = (
        {
            "status": "success",
            "msg": "",
            "payload": {
                "query_id": "DUMMY_QUERY_ID",
                "query_params": {
                    "aggregation_unit": "DUMMY_AGGREGATION",
                    "query_kind": "DUMMY_QUERY_KIND",
                },
            },
        },
        {
            "status": "success",
            "msg": "",
            "payload": {"query_id": "DUMMY_QUERY_ID", "sql": "SELECT 1;"},
        },
    )
    response = await app.client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    assert 403 == response.status_code


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
    token = access_token_builder([])
    dummy_zmq_server.side_effect = (
        {
            "status": "success",
            "payload": {
                "query_id": "5ffe4a96dbe33a117ae9550178b81836",
                "query_params": {
                    "aggregation_unit": "DUMMY_AGGREGATION",
                    "query_kind": "dummy_query_kind",
                },
            },
        },
        {
            "status": "success",
            "msg": "",
            "payload": {"query_id": "DUMMY_QUERY_ID", "query_kind": "dummy_query_kind"},
        },
    )
    response = await app.client.get(route, headers={"Authorization": f"Bearer {token}"})
    assert 403 == response.status_code
    access_logs = app.log_capture().access
    assert 3 == len(access_logs)  # One access log, two query logs
    assert "CLAIMS_VERIFICATION_FAILED" == access_logs[2]["event"]
    assert "test" == access_logs[0]["request"]["user"]
    assert "test" == access_logs[1]["request"]["user"]
    assert "test" == access_logs[2]["request"]["user"]
    assert (
        access_logs[0]["request"]["request_id"]
        == access_logs[1]["request"]["request_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("query_kind", query_kinds)
async def test_access_logs_post(
    query_kind, app, access_token_builder, dummy_zmq_server
):
    """
    Test that access logs are written for attempted unauthorized access to 'run' route.

    """
    token = access_token_builder([])
    response = await app.client.post(
        f"/api/0/run",
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": query_kind, "aggregation_unit": "admin3"},
    )
    assert 403 == response.status_code

    log_lines = app.log_capture().access
    assert 3 == len(log_lines)  # One access log, two query logs
    assert log_lines[2]["json_payload"]["query_kind"] == query_kind
    assert "CLAIMS_VERIFICATION_FAILED" == log_lines[2]["event"]
    assert "test" == log_lines[0]["request"]["user"]
    assert "test" == log_lines[1]["request"]["user"]
    assert "test" == log_lines[2]["request"]["user"]
    assert (
        log_lines[0]["request"]["request_id"] == log_lines[1]["request"]["request_id"]
    )
