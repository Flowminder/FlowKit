# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from unittest.mock import Mock

from flowclient.api_query import APIQuery


def test_query_run():
    """
    Test that the 'run' method runs the query and records the query ID internally.
    """
    connection_mock = Mock()
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    query_spec = {"query_kind": "dummy_query"}
    query = APIQuery(connection=connection_mock, parameters=query_spec)
    assert not hasattr(query, "_query_id")
    query.run()
    connection_mock.post_json.assert_called_once_with(route="run", data=query_spec)
    assert query._query_id == "DUMMY_ID"


def test_can_get_query_connection():
    """
    Test that 'connection' property returns the internal connection object
    (e.g. so that token can be updated).
    """
    connection_mock = Mock()
    query = APIQuery(
        connection=connection_mock, parameters={"query_kind": "dummy_query"}
    )
    assert query.connection is connection_mock


def test_cannot_replace_query_connection():
    """
    Test that 'connection' property does not allow setting a new connection
    (which could invalidate internal state)
    """
    query = APIQuery(connection=Mock(), parameters={"query_kind": "dummy_query"})
    with pytest.raises(AttributeError, match="can't set attribute"):
        query.connection = "NEW_CONNECTION"


def test_query_status():
    """
    Test that the 'status' property returns the status reported by the API.
    """
    connection_mock = Mock()
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    connection_mock.get_url.return_value = Mock(status_code=202)
    connection_mock.get_url.return_value.json.return_value = {
        "status": "executing",
        "progress": {"eligible": 0, "queued": 0, "running": 0},
    }
    query = APIQuery(
        connection=connection_mock, parameters={"query_kind": "dummy_query"}
    )
    query.run()
    assert query.status == "executing"


def test_query_status_not_running():
    """
    Test that the 'status' property returns 'not_running' if the query has not been set running.
    """
    query = APIQuery(connection=Mock(), parameters={"query_kind": "dummy_query"})
    assert query.status == "not_running"


def test_wait_until_ready(monkeypatch):
    """
    Test that wait_until_ready polls until query_is_ready returns True
    """
    reply_mock = Mock(
        json=Mock(
            return_value={
                "status": "executing",
                "progress": {"eligible": 0, "queued": 0, "running": 0},
            }
        )
    )
    ready_mock = Mock(
        side_effect=[
            (
                False,
                reply_mock,
            ),
            (True, reply_mock),
        ]
    )
    monkeypatch.setattr("flowclient.client.query_is_ready", ready_mock)
    connection_mock = Mock()
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    query = APIQuery(
        connection=connection_mock, parameters={"query_kind": "dummy_query"}
    )
    query.run()
    query.wait_until_ready()

    assert 2 == ready_mock.call_count


def test_wait_until_ready_raises():
    """
    Test that 'wait_until_ready' raises an error if the query has not been set running.
    """
    query = APIQuery(connection=Mock(), parameters={"query_kind": "dummy_query"})
    with pytest.raises(FileNotFoundError):
        query.wait_until_ready()


@pytest.mark.parametrize(
    "format,function",
    [
        ("pandas", "get_result_by_query_id"),
        ("geojson", "get_geojson_result_by_query_id"),
    ],
)
def test_query_get_result_pandas(monkeypatch, format, function):
    get_result_mock = Mock(return_value="DUMMY_RESULT")
    monkeypatch.setattr(f"flowclient.api_query.{function}", get_result_mock)
    connection_mock = Mock()
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    query = APIQuery(
        connection=connection_mock, parameters={"query_kind": "dummy_query"}
    )
    query.run()
    assert "DUMMY_RESULT" == query.get_result(format=format, poll_interval=2)
    get_result_mock.assert_called_once_with(
        connection=connection_mock,
        disable_progress=None,
        query_id="DUMMY_ID",
        poll_interval=2,
    )


def test_query_get_result_runs(monkeypatch):
    """
    Test that get_result runs the query if it's not already running.
    """
    get_result_mock = Mock(return_value="DUMMY_RESULT")
    monkeypatch.setattr(f"flowclient.api_query.get_result_by_query_id", get_result_mock)
    connection_mock = Mock()
    query_spec = {"query_kind": "dummy_query"}
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    query = APIQuery(connection=connection_mock, parameters=query_spec)
    query.get_result()
    connection_mock.post_json.assert_called_once_with(route="run", data=query_spec)


def test_query_get_result_invalid_format():
    """
    Test that get_result raises an error for format other than 'pandas' or 'geojson'.
    """
    query = APIQuery(
        connection="DUMMY_CONNECTION", parameters={"query_kind": "dummy_query"}
    )
    with pytest.raises(ValueError):
        query.get_result(format="INVALID_FORMAT")
