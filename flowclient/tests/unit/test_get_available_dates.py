# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest
from flowclient.client import get_available_dates
from flowclient.errors import FlowclientConnectionError


@pytest.mark.parametrize("http_code", [401, 500])
def test_get_geography_error(http_code):
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = http_code
    connection_mock.get_url.return_value.json.return_value = {"msg": "MESSAGE"}
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get available dates. API returned with status code: {http_code}. Reason: MESSAGE",
    ):
        get_available_dates(connection=connection_mock, event_types=["FOOBAR"])
