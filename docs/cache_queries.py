# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file is executed in build.sh before building the docs.
# The purpose is to speed up execution of the example notebooks,
# by caching some of the queries beforehand (which is currently
# not possible using flowclient directly).

import flowmachine
import pandas as pd

flowmachine.connect()

admin1_daily_location_queries = [
    flowmachine.features.daily_location(
        date="2016-01-01", level="admin1", method="last"
    ),
    flowmachine.features.daily_location(
        date="2016-01-07", level="admin1", method="last"
    ),
]

admin3_daily_location_queries = [
    flowmachine.features.daily_location(
        date=dl_date.strftime("%Y-%m-%d"), level="admin3", method="last"
    )
    for dl_date in pd.date_range("2016-01-01", "2016-02-28", freq="D")
]

modal_location_queries = [
    flowmachine.features.ModalLocation(
        *[
            flowmachine.features.daily_location(
                date=dl_date.strftime("%Y-%m-%d"), level="admin3", method="last"
            )
            for dl_date in pd.date_range(start_date, end_date, freq="D")
        ]
    )
    for start_date, end_date in [
        ("2016-01-01", "2016-01-21"),
        ("2016-01-21", "2016-02-10"),
        ("2016-02-10", "2016-02-28"),
    ]
]

meaningful_locations_subqueries = [
    flowmachine.features.subscriber_locations(
        start="2016-01-01", stop="2016-01-07", level="versioned-site"
    ),
    flowmachine.features.CallDays(
        subscriber_locations=flowmachine.features.subscriber_locations(
            start="2016-01-01", stop="2016-01-07", level="versioned-site"
        )
    ),
    flowmachine.features.HartiganCluster(
        calldays=flowmachine.features.CallDays(
            subscriber_locations=flowmachine.features.subscriber_locations(
                start="2016-01-01", stop="2016-01-07", level="versioned-site"
            )
        ),
        radius=1.0,
        call_threshold=0,
        buffer=0,
    ),
    flowmachine.features.EventScore(
        start="2016-01-01",
        stop="2016-01-07",
        score_hour=[
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            0,
            1,
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
            -1,
            -1,
            -1,
            -1,
            -1,
        ],
        score_dow={
            "monday": 1,
            "tuesday": 1,
            "wednesday": 1,
            "thursday": 1,
            "friday": 1,
            "saturday": -1,
            "sunday": -1,
        },
        level="versioned-site",
    ),
]

meaningful_locations_queries = [
    flowmachine.features.MeaningfulLocations(
        clusters=flowmachine.features.HartiganCluster(
            calldays=flowmachine.features.CallDays(
                subscriber_locations=flowmachine.features.subscriber_locations(
                    start="2016-01-01", stop="2016-01-07", level="versioned-site"
                )
            ),
            radius=1.0,
            call_threshold=0,
            buffer=0,
        ),
        labels={
            "home": {
                "type": "Polygon",
                "coordinates": [[[-1, 1], [-1, -1], [-1e-06, -1], [-1e-06, 1]]],
            },
            "work": {
                "type": "Polygon",
                "coordinates": [[[0, 1], [0, -1], [1, -1], [1, 1]]],
            },
        },
        scores=flowmachine.features.EventScore(
            start="2016-01-01",
            stop="2016-01-07",
            score_hour=[
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                0,
                1,
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
                -1,
                -1,
                -1,
                -1,
                -1,
            ],
            score_dow={
                "monday": 1,
                "tuesday": 1,
                "wednesday": 1,
                "thursday": 1,
                "friday": 1,
                "saturday": -1,
                "sunday": -1,
            },
            level="versioned-site",
        ),
        label=label,
    )
    for label in ["home", "work"]
]

for query in (
    admin1_daily_location_queries
    + admin3_daily_location_queries
    + modal_location_queries
    + meaningful_locations_subqueries
    + meaningful_locations_queries
):
    query.store()

for query in (
    admin1_daily_location_queries
    + modal_location_queries
    + meaningful_locations_queries
):
    query.result()
