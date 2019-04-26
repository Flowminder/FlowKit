# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from marshmallow import ValidationError

from flowmachine.core.server.query_schemas import FlowmachineQuerySchema


@pytest.mark.parametrize(
    "expected_md5, query_spec",
    [
        (
            "e374eab38bd3934054c2bac589145953",
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                    "subscriber_subset": None,
                },
            },
        ),
        (
            "3f00b5c2aadf103cc54c084b7843784c",
            {
                "query_kind": "location_event_counts",
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "interval": "day",
                "aggregation_unit": "admin3",
                "direction": "both",
                "event_types": None,
                "subscriber_subset": None,
            },
        ),
        (
            "b8dce95079c342cf8343a3f384ffdbe3",
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "modal_location",
                    "aggregation_unit": "admin3",
                    "locations": (
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-01",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-02",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                    ),
                },
            },
        ),
        (
            "f63bf3284cfe6d77a9d056f71213e9a1",
            {"query_kind": "geography", "aggregation_unit": "admin3"},
        ),
        (
            "8ea8ff7faaaa3afbd83e6df3af56714e",
            {
                "query_kind": "meaningful_locations_aggregate",
                "aggregation_unit": "admin1",
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
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
                "tower_hour_of_day_scores": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                ],
                "tower_day_of_week_scores": {
                    "monday": 1,
                    "tuesday": 1,
                    "wednesday": 1,
                    "thursday": 0,
                    "friday": -1,
                    "saturday": -1,
                    "sunday": -1,
                },
                "tower_cluster_radius": 1.0,
                "tower_cluster_call_threshold": 0,
                "subscriber_subset": None,
            },
        ),
        (
            "5fcfc6aeb30e0a6e18a4376ddfe8a7e4",
            {
                "query_kind": "meaningful_locations_between_label_od_matrix",
                "aggregation_unit": "admin1",
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "label_a": "day",
                "label_b": "evening",
                "labels": {
                    "day": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                        ],
                    },
                    "evening": {
                        "type": "Polygon",
                        "coordinates": [
                            [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                        ],
                    },
                },
                "tower_hour_of_day_scores": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                ],
                "tower_day_of_week_scores": {
                    "monday": 1,
                    "tuesday": 1,
                    "wednesday": 1,
                    "thursday": 0,
                    "friday": -1,
                    "saturday": -1,
                    "sunday": -1,
                },
                "tower_cluster_radius": 1.0,
                "tower_cluster_call_threshold": 0,
                "subscriber_subset": None,
            },
        ),
        (
            "faef1d0f53675df711aea0e958238bfa",
            {
                "query_kind": "meaningful_locations_between_dates_od_matrix",
                "aggregation_unit": "admin1",
                "start_date_a": "2016-01-01",
                "end_date_a": "2016-01-02",
                "start_date_b": "2016-01-01",
                "end_date_b": "2016-01-05",
                "label": "unknown",
                "labels": {
                    "day": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                        ],
                    },
                    "evening": {
                        "type": "Polygon",
                        "coordinates": [
                            [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                        ],
                    },
                },
                "tower_hour_of_day_scores": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                ],
                "tower_day_of_week_scores": {
                    "monday": 1,
                    "tuesday": 1,
                    "wednesday": 1,
                    "thursday": 0,
                    "friday": -1,
                    "saturday": -1,
                    "sunday": -1,
                },
                "tower_cluster_radius": 1.0,
                "tower_cluster_call_threshold": 2,
                "subscriber_subset": None,
            },
        ),
    ],
)
def test_construct_query(expected_md5, query_spec):
    """
    Test that expected query objects are constructed by construct_query_object
    """
    obj = FlowmachineQuerySchema().load(query_spec)
    assert expected_md5 == obj.query_id


# TODO: we should re-think how we want to test invalid values, now that these are validated using marshmallow
def test_wrong_geography_aggregation_unit_raises_error():
    """
    Test that an invalid aggregation unit in a geography query raises an InvalidGeographyError
    """
    with pytest.raises(
        ValidationError,
        match="aggregation_unit.*Must be one of: admin0, admin1, admin2, admin3.",
    ):
        _ = FlowmachineQuerySchema().load(
            {"query_kind": "geography", "aggregation_unit": "DUMMY_AGGREGATION_UNIT"}
        )
