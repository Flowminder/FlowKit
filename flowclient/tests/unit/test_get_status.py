# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

from flowclient.client import get_status, FlowclientConnectionError


@pytest.mark.parametrize(
    "code,status", [(202, "queued"), (202, "executing"), (303, "completed")],
)
def test_get_status(code, status):
    """
    Test that get_status reports the status returned by the API for completed or running queries.
    """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=code)
    con_mock.get_url.return_value.json.return_value = {"status": status}
    status_returned = get_status(connection=con_mock, query_id="foo")
    assert status_returned == status


def test_get_status_404():
    """ Test that get_status reports that a query is not running. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=404)
    status_returned = get_status(connection=con_mock, query_id="foo")
    assert status_returned == "not running"


def test_get_status_raises():
    """ Test that get_status raises an error for a status code other than 202, 303 or 404. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=500)
    with pytest.raises(FlowclientConnectionError):
        get_status(connection=con_mock, query_id="foo")
