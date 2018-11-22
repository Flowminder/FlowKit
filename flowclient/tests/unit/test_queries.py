# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock, PropertyMock, call

import pytest
import flowclient
from flowclient.client import (
    Connection,
    FlowclientConnectionError,
    get_result_by_query_id,
    get_result,
)


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
        connection_mock, {"query_kind": "query_type", "params": {"param": "value"}}
    )
    # Should request the query by id
    dummy_method.assert_called_with(connection_mock, "99")


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

    df = get_result_by_query_id(connection_mock, "99")

    # Query id should be requested
    assert call("poll/99") in connection_mock.get_url.call_args_list

    # Query json should be requested
    assert call("foo/Test") in connection_mock.get_url.call_args_list
    # Query result should contain the id, and the dataframe
    assert 1 == len(df)
    assert "foo" == df.name[0]


@pytest.mark.parametrize("http_code", [401, 404, 418, 400])
def test_get_result_by_id_error(http_code, token):
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = http_code
    with pytest.raises(FlowclientConnectionError):
        get_result_by_query_id(connection_mock, "99")


def test_get_result_by_id_poll_loop(monkeypatch):
    """
    Test requesting a query polls.
    """
    ready_mock = Mock(side_effect=[(False, None), StopIteration])
    monkeypatch.setattr("flowclient.client.query_is_ready", ready_mock)
    with pytest.raises(StopIteration):
        get_result_by_query_id("placeholder", "99")

    assert 2 == ready_mock.call_count
