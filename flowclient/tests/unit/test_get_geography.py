# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock, MagicMock, call

import pytest
import flowclient
from flowclient.client import get_geography
from flowclient.errors import FlowclientConnectionError


def test_get_geography(token):
    """
    Test that getting geography returns the returned dict
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = 200
    connection_mock.get_url.return_value.json.return_value = {"some": "json"}
    gj = get_geography(connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION")
    assert {"some": "json"} == gj


@pytest.mark.parametrize("http_code", [401, 404, 418, 400])
def test_get_geography_error(http_code, token):
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = http_code
    connection_mock.get_url.return_value.json.return_value = {"msg": "MESSAGE"}
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get result. API returned with status code: {http_code}. Reason: MESSAGE",
    ):
        get_geography(connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION")


def test_get_geography_no_msg_error(token):
    """
    A response with an unexpected http code and no "msg" should raise a FlowclientConnectionError.
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = 404
    connection_mock.get_url.return_value.json.return_value = {}
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get result. API returned with status code: 404.",
    ):
        get_geography(connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION")
