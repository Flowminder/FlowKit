# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file is executed in build.sh before building the docs.
# The purpose is to speed up execution of the example notebooks,
# by caching some of the queries beforehand (which is currently
# not possible using flowclient directly).

import flowmachine
import pandas as pd
import networkx as nx
import concurrent.futures

flowmachine.connect()

print("Constructing query objects")

daily_location_queries = [
    flowmachine.features.daily_location(
        date="2016-01-01", level="admin1", method="last"
    ),
    flowmachine.features.daily_location(
        date="2016-01-07", level="admin1", method="last"
    ),
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

print("Generating full dependency graph")
dependency_graphs = [
    flowmachine.utils.calculate_dependency_graph(q)
    for q in daily_location_queries
    + modal_location_queries
    + meaningful_locations_queries
]
full_graph = dependency_graphs.pop()
for graph in dependency_graphs:
    full_graph.update(graph)

print("Storing all queries and dependencies")
all_query_stores = []
for query in reversed(list(nx.topological_sort(full_graph))):
    try:
        all_query_stores.append(full_graph.nodes[query]["query_object"].store())
    except ValueError:
        # Some dependencies cannot be stored
        pass


print("Waiting for queries to finish")
concurrent.futures.wait(all_query_stores)
