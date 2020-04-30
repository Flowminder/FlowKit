# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock, PropertyMock, MagicMock, call

import pytest
import flowclient
import flowclient.client
from flowclient.client import (
    get_result_by_query_id,
    get_result,
    query_is_ready,
)
from flowclient.errors import FlowclientConnectionError
from flowclient import Connection


def test_get_result_by_params(monkeypatch, token):
    """
    Test requesting a query based on params makes the right calls.
    """
    connection_mock = Mock()
    connection_mock.post_json.return_value.status_code = 202
    connection_mock.post_json.return_value.headers = {"Location": "/99"}
    dummy_method = Mock()
    monkeypatch.setattr(flowclient.client, "get_result_by_query_id", dummy_method)
    get_result(
        connection=connection_mock,
        query_spec={"query_kind": "query_type", "params": {"param": "value"}},
    )
    # Should request the query by id
    dummy_method.assert_called_with(
        connection=connection_mock, disable_progress=None, query_id="99"
    )


def test_get_result_by_id(token):
    """
    Test requesting a query by id makes the right calls.
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.json.return_value = {
        "query_id": "99",
        "query_result": [{"name": "foo"}],
    }
    type(connection_mock.get_url.return_value).status_code = PropertyMock(
        side_effect=(303, 200)
    )
    connection_mock.get_url.return_value.headers = {"Location": "/api/0/foo/Test"}

    df = get_result_by_query_id(connection=connection_mock, query_id="99")

    # Query id should be requested
    assert call(route="poll/99") in connection_mock.get_url.call_args_list

    # Query json should be requested
    assert call(route="foo/Test") in connection_mock.get_url.call_args_list
    # Query result should contain the id, and the dataframe
    assert 1 == len(df)
    assert "foo" == df.name[0]


@pytest.mark.parametrize("http_code", [401, 404, 418, 400])
def test_get_result_by_id_error(monkeypatch, http_code, token):
    """
    Any unexpected http code should raise an exception.
    """
    dummy_reply = MagicMock()
    dummy_reply.headers.__getitem__.return_value = "/api/0/DUMMY_LOCATION"
    monkeypatch.setattr(
        flowclient.client,
        "query_is_ready",
        lambda connection, query_id: (True, dummy_reply),
    )
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = http_code
    connection_mock.get_url.return_value.json.return_value = {"msg": "MESSAGE"}
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get result. API returned with status code: {http_code}. Reason: MESSAGE",
    ):
        get_result_by_query_id(connection=connection_mock, query_id="99")
    assert call(route="DUMMY_LOCATION") in connection_mock.get_url.call_args_list


def test_get_result_by_id_poll_loop(monkeypatch):
    """
    Test requesting a query polls.
    """
    ready_mock = Mock(
        side_effect=[
            (
                False,
                Mock(
                    json=Mock(
                        return_value=dict(
                            progress=dict(eligible=2, running=2, queued=0)
                        )
                    )
                ),
            ),
            StopIteration,
        ]
    )
    monkeypatch.setattr("flowclient.client.query_is_ready", ready_mock)
    with pytest.raises(StopIteration):
        get_result_by_query_id(connection="placeholder", query_id="99")

    assert 2 == ready_mock.call_count
