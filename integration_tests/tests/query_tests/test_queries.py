# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import geojson
import flowclient
from flowclient.client import get_result

import pytest


@pytest.mark.parametrize(
    "query_kind, params",
    [
        (
            "spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                }
            },
        ),
        (
            "spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "most-common",
                    "subscriber_subset": None,
                }
            },
        ),
        (
            "unique_subscriber_counts",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin3",
            },
        ),
        (
            "total_network_objects",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin3",
            },
        ),
        (
            "total_network_objects",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin3",
                "total_by": "day",
            },
        ),
        (
            "location_introversion",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin3",
            },
        ),
        (
            "location_introversion",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin3",
                "direction": "in",
            },
        ),
        (
            "aggregate_network_objects",
            {
                "total_network_objects": {
                    "query_kind": "total_network_objects",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "aggregation_unit": "admin3",
                },
                "statistic": "median",
                "aggregate_by": "day",
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "metric": {
                    "query_kind": "radius_of_gyration",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                },
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.radius_of_gyration(
                    start_date="2016-01-01", end_date="2016-01-02"
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.nocturnal_events(
                    start="2016-01-01", stop="2016-01-02", hours=(20, 4)
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.subscriber_degree(
                    start="2016-01-01", stop="2016-01-02", direction="both"
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.unique_location_counts(
                    start_date="2016-01-01",
                    end_date="2016-01-02",
                    aggregation_unit="admin3",
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.topup_amount(
                    start="2016-01-01", stop="2016-01-02", statistic="avg"
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.event_count(
                    start="2016-01-01",
                    stop="2016-01-02",
                    direction="both",
                    event_types=["sms", "calls"],
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.displacement(
                    start="2016-01-01",
                    stop="2016-01-02",
                    statistic="avg",
                    reference_location=flowclient.daily_location(
                        date="2016-01-01", aggregation_unit="lon-lat", method="last"
                    ),
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.pareto_interactions(
                    start="2016-01-01", stop="2016-01-02", proportion="0.8"
                ),
            },
        ),
        (
            "spatial_aggregate",
            {
                "locations": flowclient.modal_location_from_dates(
                    start_date="2016-01-01",
                    end_date="2016-01-03",
                    aggregation_unit="admin3",
                    method="last",
                )
            },
        ),
        (
            "spatial_aggregate",
            {
                "locations": {
                    "query_kind": "modal_location",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-01",
                            "aggregation_unit": "admin3",
                            "method": "last",
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-02",
                            "aggregation_unit": "admin3",
                            "method": "last",
                        },
                    ],
                }
            },
        ),
        (
            "flows",
            {
                "from_location": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "to_location": {
                    "query_kind": "daily_location",
                    "date": "2016-01-04",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
            },
        ),
        (
            "flows",
            {
                "from_location": {
                    "query_kind": "modal_location",
                    "locations": [
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-01",
                            "aggregation_unit": "admin3",
                            "method": "last",
                        },
                        {
                            "query_kind": "daily_location",
                            "date": "2016-01-02",
                            "aggregation_unit": "admin3",
                            "method": "last",
                        },
                    ],
                },
                "to_location": {
                    "query_kind": "daily_location",
                    "date": "2016-01-04",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
            },
        ),
        (
            "meaningful_locations_aggregate",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin1",
                "label": "unknown",
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
        (
            "meaningful_locations_between_label_od_matrix",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "aggregation_unit": "admin1",
                "label_a": "unknown",
                "label_b": "evening",
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
        (
            "location_event_counts",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "count_interval": "day",
                "aggregation_unit": "admin3",
                "direction": "both",
                "event_types": ["calls", "sms"],
                "subscriber_subset": None,
            },
        ),
        (
            "location_event_counts",
            {
                "start_date": "2016-01-01",
                "end_date": "2016-01-02",
                "count_interval": "day",
                "aggregation_unit": "admin3",
                "direction": "both",
                "event_types": None,
                "subscriber_subset": None,
            },
        ),
        (
            "meaningful_locations_between_dates_od_matrix",
            {
                "start_date_a": "2016-01-01",
                "end_date_a": "2016-01-02",
                "start_date_b": "2016-01-01",
                "end_date_b": "2016-01-05",
                "aggregation_unit": "admin1",
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
        (
            "joined_spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "metric": {
                    "query_kind": "topup_balance",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "statistic": "avg",
                },
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.topup_balance(
                    start_date="2016-01-01", end_date="2016-01-02", statistic="avg"
                ),
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "metric": {
                    "query_kind": "handset",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "characteristic": "hnd_type",
                    "method": "last",
                },
                "method": "distr",
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": flowclient.daily_location(
                    date="2016-01-01", aggregation_unit="admin3", method="last"
                ),
                "metric": flowclient.handset(
                    start_date="2016-01-01",
                    end_date="2016-01-02",
                    characteristic="brand",
                    method="last",
                ),
                "method": "distr",
            },
        ),
        (
            "spatial_aggregate",
            {
                "locations": flowclient.random_sample(
                    query=flowclient.daily_location(
                        date="2016-01-01",
                        aggregation_unit="admin3",
                        method="most-common",
                    ),
                    size=10,
                    seed=0.5,
                )
            },
        ),
        (
            "spatial_aggregate",
            {
                "locations": flowclient.random_sample(
                    query=flowclient.daily_location(
                        date="2016-01-01",
                        aggregation_unit="admin3",
                        method="most-common",
                    ),
                    sampling_method="bernoulli",
                    fraction=0.5,
                    estimate_count=False,
                    seed=0.2,
                )
            },
        ),
    ],
)
def test_run_query(query_kind, params, universal_access_token, flowapi_url):
    """
    Test that queries can be run, and return a QueryResult object.
    """
    query_spec = getattr(flowclient, query_kind)(**params)
    con = flowclient.Connection(url=flowapi_url, token=universal_access_token)

    get_result(connection=con, query=query_spec)
    # Ideally we'd check the contents, but several queries will be totally redacted and therefore empty
    # so we can only check it runs without erroring


def test_geo_result(universal_access_token, flowapi_url):
    query_spec = flowclient.joined_spatial_aggregate(
        **{
            "locations": flowclient.daily_location(
                date="2016-01-01", aggregation_unit="admin3", method="last"
            ),
            "metric": flowclient.handset(
                start_date="2016-01-01",
                end_date="2016-01-02",
                characteristic="brand",
                method="last",
            ),
            "method": "distr",
        }
    )
    con = flowclient.Connection(url=flowapi_url, token=universal_access_token)

    result = flowclient.get_geojson_result(connection=con, query=query_spec)
    assert geojson.GeoJSON(result).is_valid


@pytest.mark.parametrize(
    "query_kind, params",
    [
        (
            "joined_spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "metric": {
                    "query_kind": "topup_balance",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "statistic": "avg",
                },
                "method": "distr",
            },
        ),
        (
            "joined_spatial_aggregate",
            {
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "method": "last",
                },
                "metric": {
                    "query_kind": "handset",
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "characteristic": "hnd_type",
                    "method": "last",
                },
                "method": "avg",
            },
        ),
    ],
)
def test_fail_query_incorrect_parameters(
    query_kind, params, universal_access_token, flowapi_url
):
    """
    Test that queries fail with incorrect parameters.
    """
    query_spec = getattr(flowclient, query_kind)(**params)
    con = flowclient.Connection(url=flowapi_url, token=universal_access_token)
    with pytest.raises(
        flowclient.client.FlowclientConnectionError, match="Must be one of:"
    ):
        result_dataframe = get_result(connection=con, query=query_spec)


def test_get_geography(access_token_builder, flowapi_url):
    """
    Test that queries can be run, and return a GeoJSON dict.
    """
    con = flowclient.Connection(
        url=flowapi_url,
        token=access_token_builder(["get_result&geography.aggregation_unit.admin3"]),
    )
    result_geojson = flowclient.get_geography(connection=con, aggregation_unit="admin3")
    assert "FeatureCollection" == result_geojson["type"]
    assert 0 < len(result_geojson["features"])
    feature0 = result_geojson["features"][0]
    assert "Feature" == feature0["type"]
    assert "pcod" in feature0["properties"]
    assert "MultiPolygon" == feature0["geometry"]["type"]
    assert list == type(feature0["geometry"]["coordinates"])
    assert 0 < len(feature0["geometry"]["coordinates"])


@pytest.mark.parametrize(
    "event_types, expected_result",
    [
        (
            ["sms", "forwards"],
            {
                "sms": [
                    "2016-01-01",
                    "2016-01-02",
                    "2016-01-03",
                    "2016-01-04",
                    "2016-01-05",
                    "2016-01-06",
                    "2016-01-07",
                ],
            },
        ),
        (
            None,
            {
                "calls": [
                    "2016-01-01",
                    "2016-01-02",
                    "2016-01-03",
                    "2016-01-04",
                    "2016-01-05",
                    "2016-01-06",
                    "2016-01-07",
                ],
                "mds": [
                    "2016-01-01",
                    "2016-01-02",
                    "2016-01-03",
                    "2016-01-04",
                    "2016-01-05",
                    "2016-01-06",
                    "2016-01-07",
                ],
                "sms": [
                    "2016-01-01",
                    "2016-01-02",
                    "2016-01-03",
                    "2016-01-04",
                    "2016-01-05",
                    "2016-01-06",
                    "2016-01-07",
                ],
                "topups": [
                    "2016-01-01",
                    "2016-01-02",
                    "2016-01-03",
                    "2016-01-04",
                    "2016-01-05",
                    "2016-01-06",
                    "2016-01-07",
                ],
            },
        ),
    ],
)
def test_get_available_dates(
    event_types, expected_result, access_token_builder, flowapi_url
):
    """
    Test that queries can be run, and return the expected JSON result.
    """
    con = flowclient.Connection(
        url=flowapi_url, token=access_token_builder(["get_result&available_dates"]),
    )
    result = flowclient.get_available_dates(connection=con, event_types=event_types)
    assert expected_result == result
