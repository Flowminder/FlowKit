# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock, MagicMock, call

import pytest
import flowclient
from flowclient.client import FlowclientConnectionError, get_geography


def test_get_geography(token):
    """
    Test that getting geography returns a correct GeoDataFrame
    """
    connection_mock = Mock()
    connection_mock.get_url.return_value.status_code = 200
    connection_mock.get_url.return_value.json.return_value = {
        "properties": {"crs": "DUMMY_CRS"},
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1.0, 0.0]},
                "properties": {},
            },
        ],
    }
    gdf = get_geography(connection_mock, "DUMMY_AGGREGATION")
    assert 2 == len(gdf)
    assert "DUMMY_CRS" == gdf.crs


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
        get_geography(connection_mock, "DUMMY_AGGREGATION")
