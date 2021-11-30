# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import pytest
from marshmallow import ValidationError

from flowmachine.core.server.query_schemas import FlowmachineQuerySchema
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema


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
    ]

    def get_query_id_for_query_spec(query_spec):
        return FlowmachineQuerySchema().load(query_spec).query_id

    query_ids_and_specs_as_json_string = json.dumps(
        {get_query_id_for_query_spec(spec): spec for spec in query_specs_to_test},
        indent=2,
    )

    diff_reporter(query_ids_and_specs_as_json_string)


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


@pytest.fixture
def location_list_unmatched_agg():
    return [
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
    ]


def test_unmatching_spatial_unit_raises_error_daily(location_list_unmatched_agg):
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


def test_unmatching_spatial_unit_raises_error_modal(location_list_unmatched_agg):
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
