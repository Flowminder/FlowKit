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


def test_get_result_by_params(monkeypatch, api_mock, token):
    """
    Test requesting a query based on params makes the right calls.
    """
    api_mock.post.return_value.status_code = 202
    api_mock.post.return_value.headers = {"Location": "/99"}
    c = Connection("foo", token)
    dummy_method = Mock()
    monkeypatch.setattr(flowclient.client, "get_result_by_query_id", dummy_method)
    get_result(c, {"query_kind": "query_type", "params": {"param": "value"}})
    # Should request the query by id
    dummy_method.assert_called_with(c, "99")


def test_get_result_by_id(api_response, api_mock, token):
    """
    Test requesting a query by id makes the right calls.
    """
    api_response.json.return_value = {
        "query_id": "99",
        "query_result": [{"name": "foo"}],
    }
    type(api_response).status_code = PropertyMock(side_effect=(303, 200))
    api_response.headers = {"Location": "/Test"}
    c = Connection("foo", token)

    df = get_result_by_query_id(c, "99")

    # Query id should be requested
    assert (
        call("foo/api/0/poll/99", allow_redirects=False) in api_mock.get.call_args_list
    )

    # Query json should be requested
    assert call("foo/Test", allow_redirects=False) in api_mock.get.call_args_list
    # Query result should contain the id, and the dataframe
    assert 1 == len(df)
    assert "foo" == df.name[0]


@pytest.mark.parametrize(
    "http_code, exception", [(401, FlowclientConnectionError), (404, FileNotFoundError)]
)
def test_get_result_by_id_error(http_code, exception, api_response, token):
    """
    Test requesting a query by id raises appropriate exceptions.
    """
    api_response.status_code = http_code
    c = Connection("foo", token)
    with pytest.raises(exception):
        get_result_by_query_id(c, "99")


def test_get_result_by_id_poll_loop(monkeypatch):
    """
    Test requesting a query polls.
    """
    ready_mock = Mock(side_effect=[(False, None), StopIteration])
    monkeypatch.setattr("flowclient.client.query_is_ready", ready_mock)
    with pytest.raises(StopIteration):
        get_result_by_query_id("placeholder", "99")

    assert 2 == ready_mock.call_count
