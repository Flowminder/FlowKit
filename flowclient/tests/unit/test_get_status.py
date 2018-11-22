# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

from flowclient.client import get_status


def test_get_status_reports_running():
    """ Test that status code 202 is interpreted as query running. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=202)
    status = get_status(con_mock, "foo")
    assert status == "Running"


def test_get_status_reports_finished():
    """ Test that status code 303 is interpreted as query finished. """
    con_mock = Mock()
    con_mock.get_url.return_value = Mock(status_code=303)
    status = get_status(con_mock, "foo")
    assert status == "Finished"
