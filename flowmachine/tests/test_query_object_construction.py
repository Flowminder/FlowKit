# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import pytest
from marshmallow import ValidationError

from flowmachine.core.server.query_schemas import FlowmachineQuerySchema
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.core.server.query_schemas.mobility_classification import (
    MobilityClassificationSchema,
)
from flowmachine.core.server.query_schemas.coalesced_location import (
    CoalescedLocationSchema,
)


def test_construct_query(diff_reporter):
    """
    Test that query objects constructed by construct_query_object() have the expected query ids.
    """
    query_specs_to_test = [
        {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "method": "last",
                "event_types": ["calls", "sms"],
                "subscriber_subset": None,
                "sampling": {
                    "sampling_method": "bernoulli",
                    "size": 10,
                    "seed": 0.5,
                    "fraction": None,
                    "estimate_count": False,
                },
            },
        },
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
        {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "method": "last",
                "event_types": None,
                "subscriber_subset": None,
                "sampling": None,
            },
        },
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
        {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "modal_location",
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
        {"query_kind": "geography", "aggregation_unit": "admin3"},
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
            "event_types": None,
            "subscriber_subset": None,
        },
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
            "event_types": ["calls", "sms"],
            "subscriber_subset": None,
        },
        {
            "query_kind": "flows",
            "from_location": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "method": "last",
            },
            "to_location": {
                "query_kind": "unique_locations",
                "start_date": "2016-01-01",
                "end_date": "2016-01-04",
                "aggregation_unit": "admin3",
            },
            "join_type": "left outer",
        },
        {
            "query_kind": "flows",
            "from_location": {
                "query_kind": "majority_location",
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
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
                    ],
                },
            },
            "to_location": {
                "query_kind": "majority_location",
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-04",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-05",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                    ],
                },
            },
        },
        {
            "query_kind": "labelled_spatial_aggregate",
            "locations": {
                "query_kind": "coalesced_location",
                "preferred_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "fallback_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
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
                    ],
                },
                "weight_threshold": 2,
            },
            "labels": {
                "query_kind": "mobility_classification",
                "locations": [
                    {
                        "query_kind": "coalesced_location",
                        "preferred_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "fallback_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "subscriber_location_weights": {
                            "query_kind": "location_visits",
                            "locations": [
                                {
                                    "query_kind": "daily_location",
                                    "date": "2016-01-05",
                                    "aggregation_unit": "admin3",
                                    "method": "last",
                                    "subscriber_subset": None,
                                },
                                {
                                    "query_kind": "daily_location",
                                    "date": "2016-01-06",
                                    "aggregation_unit": "admin3",
                                    "method": "last",
                                    "subscriber_subset": None,
                                },
                            ],
                        },
                        "weight_threshold": 2,
                    },
                    {
                        "query_kind": "coalesced_location",
                        "preferred_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "fallback_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "subscriber_location_weights": {
                            "query_kind": "location_visits",
                            "locations": [
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
                            ],
                        },
                        "weight_threshold": 2,
                    },
                ],
                "stay_length_threshold": 2,
            },
        },
        # TODO: Use a more compact 'labelled_flows' example once such a thing is exposed
        {
            "query_kind": "labelled_flows",
            "from_location": {
                "query_kind": "coalesced_location",
                "preferred_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "fallback_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
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
                    ],
                },
                "weight_threshold": 2,
            },
            "to_location": {
                "query_kind": "coalesced_location",
                "preferred_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-03",
                                "aggregation_unit": "admin3",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-04",
                                "aggregation_unit": "admin3",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                        ],
                    },
                },
                "fallback_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-03",
                                "aggregation_unit": "admin3",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-04",
                                "aggregation_unit": "admin3",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                        ],
                    },
                },
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-03",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-04",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                    ],
                },
                "weight_threshold": 2,
            },
            "labels": {
                "query_kind": "mobility_classification",
                "locations": [
                    {
                        "query_kind": "coalesced_location",
                        "preferred_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "fallback_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "subscriber_location_weights": {
                            "query_kind": "location_visits",
                            "locations": [
                                {
                                    "query_kind": "daily_location",
                                    "date": "2016-01-05",
                                    "aggregation_unit": "admin3",
                                    "method": "last",
                                    "subscriber_subset": None,
                                },
                                {
                                    "query_kind": "daily_location",
                                    "date": "2016-01-06",
                                    "aggregation_unit": "admin3",
                                    "method": "last",
                                    "subscriber_subset": None,
                                },
                            ],
                        },
                        "weight_threshold": 2,
                    },
                    {
                        "query_kind": "coalesced_location",
                        "preferred_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "fallback_location": {
                            "query_kind": "majority_location",
                            "subscriber_location_weights": {
                                "query_kind": "location_visits",
                                "locations": [
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
                                ],
                            },
                        },
                        "subscriber_location_weights": {
                            "query_kind": "location_visits",
                            "locations": [
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
                            ],
                        },
                        "weight_threshold": 2,
                    },
                ],
                "stay_length_threshold": 2,
            },
            "join_type": "full outer",
        },
    ]

    def get_query_id_for_query_spec(query_spec):
        return FlowmachineQuerySchema().load(query_spec).query_id

    query_ids_and_specs_as_json_string = json.dumps(
        {get_query_id_for_query_spec(spec): spec for spec in query_specs_to_test},
        indent=2,
    )

    diff_reporter(query_ids_and_specs_as_json_string)


# TODO: Need one of these for every spatial aggregate kind
@pytest.mark.parametrize(
    "params, expected_aggregation_unit",
    [
        (
            dict(
                query_kind="spatial_aggregate",
                locations=dict(
                    query_kind="modal_location",
                    locations=[
                        dict(
                            query_kind="daily_location",
                            date="2016-01-01",
                            method="last",
                            aggregation_unit="admin3",
                        )
                    ],
                ),
            ),
            "admin3",
        ),
        (
            dict(
                query_kind="flows",
                from_location=dict(
                    query_kind="visited_most_days",
                    start_date="2016-01-01",
                    end_date="2016-01-07",
                    aggregation_unit="admin2",
                ),
                to_location=dict(
                    query_kind="unique_locations",
                    start_date="2016-01-07",
                    end_date="2016-01-08",
                    aggregation_unit="admin2",
                ),
            ),
            "admin2",
        ),
        (
            dict(
                query_kind="unique_subscriber_counts",
                start_date="2016-01-01",
                end_date="2016-01-02",
                aggregation_unit="lon-lat",
            ),
            "lon-lat",
        ),
        (
            dict(
                query_kind="joined_spatial_aggregate",
                locations=dict(
                    query_kind="daily_location",
                    date="2016-01-01",
                    method="last",
                    aggregation_unit="admin3",
                ),
                metric=dict(
                    query_kind="displacement",
                    start_date="2016-01-01",
                    end_date="2016-01-02",
                    statistic="avg",
                    reference_location=dict(
                        query_kind="daily_location",
                        date="2016-01-01",
                        method="last",
                        aggregation_unit="lon-lat",
                    ),
                ),
            ),
            "admin3",
        ),
    ],
)
def test_aggregation_unit_attribute(params, expected_aggregation_unit):
    """
    Test that spatially aggregated query kinds have the correct aggregation unit when deserialised
    """
    loaded_query = FlowmachineQuerySchema().load(params)
    assert loaded_query.aggregation_unit.canonical_name == expected_aggregation_unit


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


@pytest.mark.parametrize(
    "sampling, message",
    [
        (
            {"sampling_method": "bernoulli", "size": 10, "fraction": 0.2},
            "Missing data for required field.",
        ),
        (
            {"sampling_method": "bernoulli", "size": 10, "fraction": 0.2, "seed": 0.1},
            "Must provide exactly one of 'size' or 'fraction' for a random sample",
        ),
        (
            {"sampling_method": "bernoulli", "seed": 0.1},
            "Must provide exactly one of 'size' or 'fraction' for a random sample",
        ),
        (
            {"sampling_method": "bernoulli", "fraction": 1.2, "seed": 0.1},
            "Must be greater than 0.0 and less than 1.0.",
        ),
        (
            {"sampling_method": "bernoulli", "size": -1, "seed": 0.1},
            "Must be greater than or equal to 1.",
        ),
        (
            {"sampling_method": "random_ids", "size": 10, "seed": 185},
            "Must be greater than or equal to -1.0 and less than or equal to 1.0.",
        ),
    ],
)
def test_invalid_sampling_params_raises_error(sampling, message):
    query_spec = {
        "query_kind": "spatial_aggregate",
        "locations": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
            "method": "last",
            "sampling": sampling,
        },
    }
    with pytest.raises(ValidationError, match=message) as exc:
        _ = FlowmachineQuerySchema().load(query_spec)
    print(exc)


def test_unmatching_spatial_unit_raises_error_daily():
    query_spec = {
        "query_kind": "location_visits",
        "locations": [
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
                "aggregation_unit": "admin2",
                "method": "last",
                "subscriber_subset": None,
            },
        ],
    }

    with pytest.raises(ValidationError, match="same aggregation unit") as exc:
        _ = LocationVisitsSchema().load(query_spec)
    print(exc)


def test_unmatching_spatial_unit_raises_error_modal():
    query_spec = {
        "query_kind": "location_visits",
        "locations": [
            {
                "query_kind": "modal_location",
                "locations": [
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-01",
                        "aggregation_unit": "admin2",
                        "method": "last",
                        "subscriber_subset": None,
                    },
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-02",
                        "aggregation_unit": "admin2",
                        "method": "last",
                        "subscriber_subset": None,
                    },
                ],
            },
            {
                "query_kind": "modal_location",
                "locations": [
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
                ],
            },
        ],
    }
    with pytest.raises(ValidationError, match="same aggregation unit") as exc:
        _ = LocationVisitsSchema().load(query_spec)
    print(exc)


@pytest.mark.parametrize(
    "agg_unit_preferred, agg_unit_fallback, agg_unit_weights, invalid_fields",
    [
        ("admin3", "admin3", "admin1", {"subscriber_location_weights"}),
        ("admin3", "admin1", "admin1", {"fallback_location"}),
        (
            "admin3",
            "admin2",
            "admin1",
            {"fallback_location", "subscriber_location_weights"},
        ),
    ],
)
def test_mismatched_aggregation_units_coalesced_location(
    agg_unit_preferred, agg_unit_fallback, agg_unit_weights, invalid_fields
):
    query_spec = {
        "query_kind": "coalesced_location",
        "preferred_location": {
            "query_kind": "majority_location",
            "subscriber_location_weights": {
                "query_kind": "location_visits",
                "locations": [
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-01",
                        "aggregation_unit": agg_unit_preferred,
                        "method": "last",
                        "subscriber_subset": None,
                    },
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-02",
                        "aggregation_unit": agg_unit_preferred,
                        "method": "last",
                        "subscriber_subset": None,
                    },
                ],
            },
        },
        "fallback_location": {
            "query_kind": "majority_location",
            "subscriber_location_weights": {
                "query_kind": "location_visits",
                "locations": [
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-01",
                        "aggregation_unit": agg_unit_fallback,
                        "method": "last",
                        "subscriber_subset": None,
                    },
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-02",
                        "aggregation_unit": agg_unit_fallback,
                        "method": "last",
                        "subscriber_subset": None,
                    },
                ],
            },
        },
        "subscriber_location_weights": {
            "query_kind": "location_visits",
            "locations": [
                {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": agg_unit_weights,
                    "method": "last",
                    "subscriber_subset": None,
                },
                {
                    "query_kind": "daily_location",
                    "date": "2016-01-02",
                    "aggregation_unit": agg_unit_weights,
                    "method": "last",
                    "subscriber_subset": None,
                },
            ],
        },
        "weight_threshold": 2,
    }
    with pytest.raises(ValidationError, match="aggregation_unit") as exc:
        CoalescedLocationSchema().load(query_spec)
    # Check that errors were raised for the expected fields
    assert exc.value.messages.keys() == invalid_fields


def test_mismatched_aggregation_units_mobility_classification():
    query_spec = {
        "query_kind": "mobility_classification",
        "locations": [
            {
                "query_kind": "coalesced_location",
                "preferred_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "fallback_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
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
                        ],
                    },
                },
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-05",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-06",
                            "aggregation_unit": "admin3",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                    ],
                },
                "weight_threshold": 2,
            },
            {
                "query_kind": "coalesced_location",
                "preferred_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-01",
                                "aggregation_unit": "admin2",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-02",
                                "aggregation_unit": "admin2",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                        ],
                    },
                },
                "fallback_location": {
                    "query_kind": "majority_location",
                    "subscriber_location_weights": {
                        "query_kind": "location_visits",
                        "locations": [
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-01",
                                "aggregation_unit": "admin2",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                            {
                                "query_kind": "daily_location",
                                "date": "2016-01-02",
                                "aggregation_unit": "admin2",
                                "method": "last",
                                "subscriber_subset": None,
                            },
                        ],
                    },
                },
                "subscriber_location_weights": {
                    "query_kind": "location_visits",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-01",
                            "aggregation_unit": "admin2",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-02",
                            "aggregation_unit": "admin2",
                            "method": "last",
                            "subscriber_subset": None,
                        },
                    ],
                },
                "weight_threshold": 2,
            },
        ],
        "stay_length_threshold": 2,
    }
    with pytest.raises(ValidationError, match="aggregation unit") as exc:
        MobilityClassificationSchema().load(query_spec)
    # Check that error was raised for the expected field
    assert exc.value.messages.keys() == {"locations"}
