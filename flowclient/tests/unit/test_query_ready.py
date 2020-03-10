# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

from flowclient.client import query_is_ready
from flowclient.client import FlowclientConnectionError


def test_query_ready_reports_false():
    """ Test that status code 202 is interpreted as query running. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=202)
    con_mock.get_url.return_value.json.return_value = {
        "status": "completed",
        "payload": {"completed": [1, 1]},
    }
    is_ready, reply = query_is_ready(connection=con_mock, query_id="foo")
    assert not is_ready


def test_query_ready_raises():
    """ Test that status codes other than 202, 300, 401, and 404 raise a generic error. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=999)
    with pytest.raises(FlowclientConnectionError):
        query_is_ready(connection=con_mock, query_id="foo")
