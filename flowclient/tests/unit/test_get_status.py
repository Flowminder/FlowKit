# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

from flowclient.client import get_status
from flowclient.errors import FlowclientConnectionError


@pytest.mark.parametrize("running_status", ["queued", "executing"])
def test_get_status_reports_running(running_status):
    """Test that status code 202 is interpreted as query running or queued."""
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=202)
    con_mock.get_url.return_value.json.return_value = {
        "status": running_status,
        "progress": {"eligible": 0, "queued": 0, "running": 0},
    }
    status = get_status(connection=con_mock, query_id="foo")
    assert status == running_status


def test_get_status_reports_finished():
    """Test that status code 303 is interpreted as query finished."""
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=303)
    status = get_status(connection=con_mock, query_id="foo")
    assert status == "completed"


def test_get_status_404():
    """Test that get_status reports that a query is not running."""
    con_mock = Mock()
    con_mock.get_url.side_effect = FileNotFoundError("DUMMY_404")
    status_returned = get_status(connection=con_mock, query_id="foo")
    assert status_returned == "not_running"


def test_get_status_raises():
    """Test that get_status raises an error for a status code other than 202, 303 or 404."""
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=500)
    with pytest.raises(FlowclientConnectionError):
        get_status(connection=con_mock, query_id="foo")
