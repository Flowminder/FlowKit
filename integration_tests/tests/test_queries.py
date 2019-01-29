# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowclient.client import get_result

import flowclient

from .utils import permissions_types, aggregation_types


@pytest.mark.parametrize(
    "query_kind, params",
    [
        (
            "daily_location",
            {
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "daily_location_method": "last",
            },
        ),
        (
            "daily_location",
            {
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "daily_location_method": "most-common",
                "subscriber_subset": None,
            },
        ),
    ],
)
def test_run_query(query_kind, params, access_token_builder, api_host):
    """Test that queries can be run, and return a QueryResult object."""
    con = flowclient.Connection(
        api_host,
        access_token_builder(
            {
                query_kind: {
                    "permissions": permissions_types,
                    "spatial_aggregation": aggregation_types,
                }
            }
        ),
    )
    result_dataframe = get_result(con, getattr(flowclient, query_kind)(**params))
    assert 0 < len(result_dataframe)


def test_get_geography(access_token_builder, api_host):
    """Test that queries can be run, and return a GeoJSON dict."""
    con = flowclient.Connection(
        api_host,
        access_token_builder(
            {
                "geography": {
                    "permissions": permissions_types,
                    "spatial_aggregation": aggregation_types,
                }
            }
        ),
    )
    result_geojson = flowclient.get_geography(con, "admin3")
    assert "FeatureCollection" == result_geojson["type"]
    assert 0 < len(result_geojson["features"])
    feature0 = result_geojson["features"][0]
    assert "Feature" == feature0["type"]
    assert "admin3name" in feature0["properties"]
    assert "admin3pcod" in feature0["properties"]
    assert "MultiPolygon" == feature0["geometry"]["type"]
    assert list == type(feature0["geometry"]["coordinates"])
    assert 0 < len(feature0["geometry"]["coordinates"])
