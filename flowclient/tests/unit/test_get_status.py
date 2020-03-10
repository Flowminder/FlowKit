# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

from flowclient.client import get_status


@pytest.mark.parametrize("running_status", ["queued", "executing"])
def test_get_status_reports_running(running_status):
    """ Test that status code 202 is interpreted as query running or queued. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=202)
    con_mock.get_url.return_value.json.return_value = {
        "status": running_status,
        "payload": {"completed": [1, 1]},
    }
    status = get_status(connection=con_mock, query_id="foo")
    assert status == running_status


def test_get_status_reports_finished():
    """ Test that status code 303 is interpreted as query finished. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=303)
    status = get_status(connection=con_mock, query_id="foo")
    assert status == "Finished"
