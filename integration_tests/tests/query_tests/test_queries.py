# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import partial

import geojson
import flowclient

import pytest

queries = [
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        metric=flowclient.total_active_periods_spec(
            start_date="2016-01-01",
            total_periods=1,
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.most_frequent_location_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.most_frequent_location_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            hours=(0, 1),
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="most-common",
            event_types=["calls", "sms"],
            subscriber_subset=None,
        ),
    ),
    partial(
        flowclient.unique_subscriber_counts,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
    ),
    partial(
        flowclient.unique_subscriber_counts,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.total_network_objects,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
    ),
    partial(
        flowclient.total_network_objects,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
        total_by="day",
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.location_introversion,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
    ),
    partial(
        flowclient.location_introversion,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin3",
        direction="in",
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.aggregate_network_objects,
        total_network_objects=flowclient.aggregates.total_network_objects_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            aggregation_unit="admin3",
        ),
        statistic="median",
        aggregate_by="day",
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        metric=flowclient.radius_of_gyration_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        metric=flowclient.radius_of_gyration_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.nocturnal_events_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            hours=(20, 4),
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.subscriber_degree_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            direction="both",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.unique_location_counts_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            aggregation_unit="admin3",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.topup_amount_spec(
            start_date="2016-01-01", end_date="2016-01-02", statistic="avg"
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.event_count_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            direction="both",
            event_types=["sms", "calls"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.displacement_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            statistic="avg",
            reference_location=flowclient.daily_location_spec(
                date="2016-01-01", aggregation_unit="lon-lat", method="last"
            ),
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.pareto_interactions_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            proportion="0.8",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.modal_location_from_dates_spec(
            start_date="2016-01-01",
            end_date="2016-01-03",
            aggregation_unit="admin3",
            method="last",
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.modal_location_from_dates_spec(
            start_date="2016-01-01",
            end_date="2016-01-03",
            aggregation_unit="admin3",
            method="last",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.modal_location_spec(
            locations=[
                flowclient.daily_location_spec(
                    date="2016-01-01",
                    aggregation_unit="admin3",
                    method="last",
                ),
                flowclient.daily_location_spec(
                    date="2016-01-02",
                    aggregation_unit="admin3",
                    method="last",
                ),
            ],
        ),
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        to_location=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-04",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.flows,
        to_location=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        from_location=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-04",
            aggregation_unit="admin3",
            event_types=["calls", "sms"],
        ),
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-04",
            aggregation_unit="admin3",
        ),
        to_location=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-04",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        to_location=flowclient.daily_location_spec(
            date="2016-01-04",
            aggregation_unit="admin3",
            method="last",
        ),
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.modal_location_spec(
            locations=[
                flowclient.daily_location_spec(
                    date="2016-01-01",
                    aggregation_unit="admin3",
                    method="last",
                ),
                flowclient.daily_location_spec(
                    date="2016-01-02",
                    aggregation_unit="admin3",
                    method="last",
                ),
            ],
        ),
        to_location=flowclient.daily_location_spec(
            date="2016-01-04",
            aggregation_unit="admin3",
            method="last",
        ),
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        to_location=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-04",
            aggregation_unit="admin3",
        ),
        join_type="left outer",
    ),
    partial(
        flowclient.meaningful_locations_aggregate,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin1",
        label="unknown",
        tower_hour_of_day_scores=[
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
        tower_day_of_week_scores=dict(
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=0,
            friday=-1,
            saturday=-1,
            sunday=-1,
        ),
        labels=dict(
            evening=dict(
                type="Polygon",
                coordinates=[[[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]],
            ),
            day=dict(
                type="Polygon",
                coordinates=[[[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]],
            ),
        ),
    ),
    partial(
        flowclient.meaningful_locations_aggregate,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin1",
        label="unknown",
        tower_hour_of_day_scores=[
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
        tower_day_of_week_scores=dict(
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=0,
            friday=-1,
            saturday=-1,
            sunday=-1,
        ),
        labels=dict(
            evening=dict(
                type="Polygon",
                coordinates=[[[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]],
            ),
            day=dict(
                type="Polygon",
                coordinates=[[[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]],
            ),
        ),
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.meaningful_locations_between_label_od_matrix,
        start_date="2016-01-01",
        end_date="2016-01-02",
        aggregation_unit="admin1",
        label_a="unknown",
        label_b="evening",
        tower_hour_of_day_scores=[
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
        tower_day_of_week_scores=dict(
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=0,
            friday=-1,
            saturday=-1,
            sunday=-1,
        ),
        labels=dict(
            evening=dict(
                type="Polygon",
                coordinates=[[[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]],
            ),
            day=dict(
                type="Polygon",
                coordinates=[[[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]],
            ),
        ),
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.location_event_counts,
        start_date="2016-01-01",
        end_date="2016-01-02",
        count_interval="day",
        aggregation_unit="admin3",
        direction="both",
        event_types=["calls", "sms"],
        subscriber_subset=None,
    ),
    partial(
        flowclient.location_event_counts,
        start_date="2016-01-01",
        end_date="2016-01-02",
        count_interval="day",
        aggregation_unit="admin3",
        direction="both",
        event_types=None,
        subscriber_subset=None,
    ),
    partial(
        flowclient.meaningful_locations_between_dates_od_matrix,
        start_date_a="2016-01-01",
        end_date_a="2016-01-02",
        start_date_b="2016-01-01",
        end_date_b="2016-01-05",
        aggregation_unit="admin1",
        tower_hour_of_day_scores=[
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
        tower_day_of_week_scores=dict(
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=0,
            friday=-1,
            saturday=-1,
            sunday=-1,
        ),
        label="unknown",
        labels=dict(
            evening=dict(
                type="Polygon",
                coordinates=[[[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]],
            ),
            day=dict(
                type="Polygon",
                coordinates=[[[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]],
            ),
        ),
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        metric=flowclient.topup_balance_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            statistic="avg",
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.topup_balance_spec(
            start_date="2016-01-01", end_date="2016-01-02", statistic="avg"
        ),
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
        ),
        metric=flowclient.handset_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            characteristic="hnd_type",
            method="last",
        ),
        method="distr",
    ),
    partial(
        flowclient.joined_spatial_aggregate,
        locations=flowclient.daily_location_spec(
            date="2016-01-01", aggregation_unit="admin3", method="last"
        ),
        metric=flowclient.handset_spec(
            start_date="2016-01-01",
            end_date="2016-01-02",
            characteristic="brand",
            method="last",
            event_types=["calls", "sms"],
        ),
        method="distr",
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.random_sample_spec(
            query=flowclient.daily_location_spec(
                date="2016-01-01",
                aggregation_unit="admin3",
                method="most-common",
            ),
            size=10,
            seed=0.5,
        ),
    ),
    partial(
        flowclient.spatial_aggregate,
        locations=flowclient.random_sample_spec(
            query=flowclient.daily_location_spec(
                date="2016-01-01",
                aggregation_unit="admin3",
                method="most-common",
            ),
            sampling_method="bernoulli",
            fraction=0.5,
            estimate_count=False,
            seed=0.2,
        ),
    ),
    partial(
        flowclient.histogram_aggregate,
        metric=flowclient.event_count_spec(
            start_date="2016-01-01", end_date="2016-01-02"
        ),
        bins=5,
    ),
    partial(
        flowclient.active_at_reference_location_counts,
        reference_locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="most-common",
        ),
        unique_locations=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-03",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.unmoving_at_reference_location_counts,
        reference_locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="most-common",
        ),
        unique_locations=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-03",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.unmoving_counts,
        unique_locations=flowclient.unique_locations_spec(
            start_date="2016-01-01",
            end_date="2016-01-03",
            aggregation_unit="admin3",
        ),
    ),
    partial(
        flowclient.consecutive_trips_od_matrix,
        start_date="2016-01-01",
        end_date="2016-01-03",
        aggregation_unit="admin3",
    ),
    partial(
        flowclient.consecutive_trips_od_matrix,
        start_date="2016-01-01",
        end_date="2016-01-03",
        aggregation_unit="admin3",
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.trips_od_matrix,
        start_date="2016-01-01",
        end_date="2016-01-03",
        aggregation_unit="admin3",
    ),
    partial(
        flowclient.trips_od_matrix,
        start_date="2016-01-01",
        end_date="2016-01-03",
        aggregation_unit="admin3",
        event_types=["calls", "sms"],
    ),
    partial(
        flowclient.flows,
        from_location=flowclient.majority_location_spec(
            subscriber_location_weights=flowclient.location_visits_spec(
                locations=[
                    flowclient.daily_location_spec(
                        date="2016-01-01",
                        aggregation_unit="admin3",
                        method="last",
                        subscriber_subset=None,
                    ),
                    flowclient.daily_location_spec(
                        date="2016-01-02",
                        aggregation_unit="admin3",
                        method="last",
                        subscriber_subset=None,
                    ),
                ]
            ),
        ),
        to_location=flowclient.majority_location_spec(
            subscriber_location_weights=flowclient.location_visits_spec(
                locations=[
                    flowclient.daily_location_spec(
                        date="2016-01-04",
                        aggregation_unit="admin3",
                        method="last",
                        subscriber_subset=None,
                    ),
                    flowclient.daily_location_spec(
                        date="2016-01-05",
                        aggregation_unit="admin3",
                        method="last",
                        subscriber_subset=None,
                    ),
                ],
            )
        ),
    ),
    partial(
        flowclient.labelled_spatial_aggregate,
        locations=flowclient.coalesced_location_spec(
            preferred_location=flowclient.majority_location_spec(
                subscriber_location_weights=flowclient.location_visits_spec(
                    locations=[
                        flowclient.daily_location_spec(
                            date="2016-01-01",
                            aggregation_unit="admin3",
                            method="last",
                        ),
                        flowclient.daily_location_spec(
                            date="2016-01-02",
                            aggregation_unit="admin3",
                            method="last",
                        ),
                    ],
                )
            ),
            fallback_location=flowclient.majority_location_spec(
                subscriber_location_weights=flowclient.location_visits_spec(
                    locations=[
                        flowclient.daily_location_spec(
                            date="2016-01-01",
                            aggregation_unit="admin3",
                            method="last",
                        ),
                        flowclient.daily_location_spec(
                            date="2016-01-02",
                            aggregation_unit="admin3",
                            method="last",
                        ),
                    ],
                )
            ),
            subscriber_location_weights=flowclient.location_visits_spec(
                locations=[
                    flowclient.daily_location_spec(
                        date="2016-01-01",
                        aggregation_unit="admin3",
                        method="last",
                    ),
                    flowclient.daily_location_spec(
                        date="2016-01-02",
                        aggregation_unit="admin3",
                        method="last",
                    ),
                ],
            ),
            weight_threshold=2,
        ),
        labels=flowclient.mobility_classification_spec(
            locations=[
                flowclient.coalesced_location_spec(
                    preferred_location=flowclient.majority_location_spec(
                        subscriber_location_weights=flowclient.location_visits_spec(
                            locations=[
                                flowclient.daily_location_spec(
                                    date="2016-01-01",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                                flowclient.daily_location_spec(
                                    date="2016-01-02",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                            ],
                        )
                    ),
                    fallback_location=flowclient.majority_location_spec(
                        subscriber_location_weights=flowclient.location_visits_spec(
                            locations=[
                                flowclient.daily_location_spec(
                                    date="2016-01-01",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                                flowclient.daily_location_spec(
                                    date="2016-01-02",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                            ],
                        )
                    ),
                    subscriber_location_weights=flowclient.location_visits_spec(
                        locations=[
                            flowclient.daily_location_spec(
                                date="2016-01-05",
                                aggregation_unit="admin3",
                                method="last",
                            ),
                            flowclient.daily_location_spec(
                                date="2016-01-06",
                                aggregation_unit="admin3",
                                method="last",
                            ),
                        ],
                    ),
                    weight_threshold=2,
                ),
                flowclient.coalesced_location_spec(
                    preferred_location=flowclient.majority_location_spec(
                        subscriber_location_weights=flowclient.location_visits_spec(
                            locations=[
                                flowclient.daily_location_spec(
                                    date="2016-01-01",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                                flowclient.daily_location_spec(
                                    date="2016-01-02",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                            ],
                        )
                    ),
                    fallback_location=flowclient.majority_location_spec(
                        subscriber_location_weights=flowclient.location_visits_spec(
                            locations=[
                                flowclient.daily_location_spec(
                                    date="2016-01-01",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                                flowclient.daily_location_spec(
                                    date="2016-01-02",
                                    aggregation_unit="admin3",
                                    method="last",
                                ),
                            ],
                        )
                    ),
                    subscriber_location_weights=flowclient.location_visits_spec(
                        locations=[
                            flowclient.daily_location_spec(
                                date="2016-01-01",
                                aggregation_unit="admin3",
                                method="last",
                            ),
                            flowclient.daily_location_spec(
                                date="2016-01-02",
                                aggregation_unit="admin3",
                                method="last",
                            ),
                        ],
                    ),
                    weight_threshold=2,
                ),
            ],
            stay_length_threshold=2,
        ),
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection", [flowclient.Connection, flowclient.ASyncConnection]
)
@pytest.mark.parametrize(
    "query",
    queries,
    ids=lambda val: f"{val.func.__name__}({val.keywords})",
)
async def test_run_query(connection, query, universal_access_token, flowapi_url):
    """
    Test that queries can be run, and return a QueryResult object.
    """
    con = connection(url=flowapi_url, token=universal_access_token)

    if query.keywords == {
        "locations": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
            "method": "last",
            "event_types": None,
            "subscriber_subset": None,
            "mapping_table": None,
            "geom_table": None,
            "geom_table_join_column": None,
            "hours": None,
        },
        "metric": {
            "query_kind": "displacement",
            "start_date": "2016-01-01",
            "end_date": "2016-01-02",
            "statistic": "avg",
            "reference_location": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "lon-lat",
                "method": "last",
                "event_types": None,
                "subscriber_subset": None,
                "mapping_table": None,
                "geom_table": None,
                "geom_table_join_column": None,
                "hours": None,
            },
            "event_types": ["calls", "sms"],
            "subscriber_subset": None,
            "hours": None,
        },
    }:
        pytest.xfail(
            "Under new schema rules, cannot presently mix admin levels. See bug #4649"
        )

    try:
        await query(connection=con).get_result()
    except TypeError:
        query(connection=con).get_result()
    # Ideally we'd check the contents, but several queries will be totally redacted and therefore empty
    # so we can only check it runs without erroring


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection", [flowclient.Connection, flowclient.ASyncConnection]
)
async def test_geo_result(connection, universal_access_token, flowapi_url):
    query = flowclient.joined_spatial_aggregate(
        connection=connection(url=flowapi_url, token=universal_access_token),
        **{
            "locations": flowclient.daily_location_spec(
                date="2016-01-01", aggregation_unit="admin3", method="last"
            ),
            "metric": flowclient.handset_spec(
                start_date="2016-01-01",
                end_date="2016-01-02",
                characteristic="brand",
                method="last",
            ),
            "method": "distr",
        },
    )

    try:
        result = await query.get_result(format="geojson")
    except TypeError:
        result = query.get_result(format="geojson")
    assert geojson.GeoJSON(result).is_valid


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection", [flowclient.Connection, flowclient.ASyncConnection]
)
@pytest.mark.parametrize(
    "query",
    [
        partial(
            flowclient.joined_spatial_aggregate,
            locations=flowclient.daily_location_spec(
                date="2016-01-01",
                aggregation_unit="admin3",
                method="last",
            ),
            metric=flowclient.topup_balance_spec(
                start_date="2016-01-01",
                end_date="2016-01-02",
                statistic="avg",
            ),
            method="distr",
        ),
        partial(
            flowclient.joined_spatial_aggregate,
            locations=flowclient.daily_location_spec(
                date="2016-01-01",
                aggregation_unit="admin3",
                method="last",
            ),
            metric=flowclient.handset_spec(
                start_date="2016-01-01",
                end_date="2016-01-02",
                characteristic="hnd_type",
                method="last",
            ),
            method="avg",
        ),
    ],
)
async def test_fail_query_incorrect_parameters(
    connection, query, universal_access_token, flowapi_url
):
    """
    Test that queries fail with incorrect parameters.
    """
    con = connection(url=flowapi_url, token=universal_access_token)
    with pytest.raises(
        flowclient.client.FlowclientConnectionError, match="Must be one of:"
    ):
        try:
            await query(connection=con).get_result()
        except TypeError:
            query(connection=con).get_result()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection",
    [
        flowclient.Connection,
        flowclient.ASyncConnection,
    ],
)
async def test_mapping_table(connection, universal_access_token, flowapi_url):
    mapping_table = "infrastructure.mapping_table_test"
    query = flowclient.spatial_aggregate(
        connection=connection(url=flowapi_url, token=universal_access_token),
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="admin3",
            method="last",
            mapping_table=mapping_table,
        ),
    )
    try:
        result = await query.get_result()
    except TypeError:
        result = query.get_result()
    assert set(result.pcod.tolist()) == {"524 2 05 24"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection",
    [
        flowclient.Connection,
        flowclient.ASyncConnection,
    ],
)
async def test_point_mapping_table(connection, universal_access_token, flowapi_url):
    mapping_table = "geography.test_point_mapping"
    query = flowclient.spatial_aggregate(
        connection=connection(url=flowapi_url, token=universal_access_token),
        locations=flowclient.daily_location_spec(
            date="2016-01-01",
            aggregation_unit="lon-lat",
            method="last",
            mapping_table=mapping_table,
            geom_table="geography.test_cluster",
            geom_table_join_column="cluster_id",
        ),
    )
    try:
        result = await query.get_result()
    except TypeError:
        result = query.get_result()
    assert set(result.lon.tolist()) == pytest.approx({26.669991})
    assert set(result.lat.tolist()) == pytest.approx({87.85779897})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_geography(connection, module, access_token_builder, flowapi_url):
    """
    Test that queries can be run, and return a GeoJSON dict.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder(["get_result&geography.aggregation_unit.admin3"]),
    )
    try:
        result_geojson = await module.get_geography(
            connection=con, aggregation_unit="admin3"
        )
    except TypeError:
        result_geojson = module.get_geography(connection=con, aggregation_unit="admin3")
    assert "FeatureCollection" == result_geojson["type"]
    assert 0 < len(result_geojson["features"])
    feature0 = result_geojson["features"][0]
    assert "Feature" == feature0["type"]
    assert "pcod" in feature0["properties"]
    assert "MultiPolygon" == feature0["geometry"]["type"]
    assert list == type(feature0["geometry"]["coordinates"])
    assert 0 < len(feature0["geometry"]["coordinates"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
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
async def test_get_available_dates(
    connection, module, event_types, expected_result, access_token_builder, flowapi_url
):
    """
    Test that queries can be run, and return the expected JSON result.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder(["get_result&available_dates"]),
    )
    try:
        result = await module.get_available_dates(
            connection=con, event_types=event_types
        )
    except TypeError:
        result = module.get_available_dates(connection=con, event_types=event_types)
    assert expected_result == result
