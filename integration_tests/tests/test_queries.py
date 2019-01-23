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
        (
            "meaningful_locations_aggregate",
            {
                "start_date": "2016-01-01",
                "stop_date": "2016-01-02",
                "aggregation_unit": "admin1",
                "label": "unknown",
                "labels": {
                    "evening": {
                        "type": "Polygon",
                        "coordinates": [
                            [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                        ],
                    },
                    "day": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                        ],
                    },
                },
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
    query_spec = getattr(flowclient, query_kind)(**params)
    result_dataframe = get_result(con, query_spec)
    assert 0 < len(result_dataframe)
