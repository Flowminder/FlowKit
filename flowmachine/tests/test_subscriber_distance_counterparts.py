# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceCounterparts().
"""


from flowmachine.features.subscriber import DistanceCounterparts
from flowmachine.features.utilities import EventsTablesUnion
from flowmachine.features.spatial.distance_matrix import DistanceMatrix

import pytest


@pytest.fixture()
def distance_matrix(get_dataframe):
    """ Calculates the distance matrix between cells. """
    return get_dataframe(DistanceMatrix())


@pytest.fixture()
def distance_counterparts(get_dataframe, distance_matrix):
    """ Returns a dataframe with the distance from subscriber and counterpart
    for each event. """

    def _distance_counterparts(start, stop, direction, subset):
        events_a_query = EventsTablesUnion(
            start,
            stop,
            columns=["msisdn", "id", "location_id", "outgoing"],
            subscriber_subset=subset.index.values,
            subscriber_identifier="msisdn",
        )
        events_a = get_dataframe(events_a_query)

        events_b_query = EventsTablesUnion(
            start,
            stop,
            columns=["id", "location_id", "outgoing"],
            subscriber_subset=subset.index.values,
            subscriber_identifier="msisdn_counterpart",
        )
        events_b = get_dataframe(events_b_query)

        events = events_a.merge(events_b, on="id", suffixes=("_A", "_B"))

        if direction == "out":
            events = events[events.outgoing_A == True]
        elif direction == "in":
            events = events[events.outgoing_A == False]
        events = events.query("outgoing_A != outgoing_B")

        events = events.merge(
            distance_matrix,
            left_on=("location_id_A", "location_id_B"),
            right_on=("location_id_from", "location_id_to"),
        )

        return events.loc[:, ["subscriber", "distance"]].groupby("subscriber")

    return _distance_counterparts


def test_distance_counterparts(get_dataframe, distance_counterparts):
    """
    Test some hand-picked results for DistanceCounterparts.
    """
    query = DistanceCounterparts("2016-01-01", "2016-01-07")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    dist = distance_counterparts("2016-01-01", "2016-01-07", "both", sample).mean()
    joined = sample.join(dist)
    assert joined["distance_avg"].to_dict() == pytest.approx(
        joined["distance"].to_dict()
    )

    query = DistanceCounterparts("2016-01-01", "2016-01-07", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    dist = distance_counterparts("2016-01-01", "2016-01-07", "out", sample).mean()
    joined = sample.join(dist)
    assert joined["distance_avg"].to_dict() == pytest.approx(
        joined["distance"].to_dict()
    )

    query = DistanceCounterparts("2016-01-03", "2016-01-05", direction="in")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    dist = distance_counterparts("2016-01-03", "2016-01-05", "in", sample).mean()
    joined = sample.join(dist)
    assert joined["distance_avg"].to_dict() == pytest.approx(
        joined["distance"].to_dict()
    )

    query = DistanceCounterparts(
        "2016-01-03",
        "2016-01-05",
        direction="in",
        subscriber_subset=sample.index.values,
    )
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    dist = distance_counterparts("2016-01-03", "2016-01-05", "in", sample).mean()
    joined = sample.join(dist)
    assert joined["distance_avg"].to_dict() == pytest.approx(
        joined["distance"].to_dict()
    )

    query = DistanceCounterparts("2016-01-01", "2016-01-05", statistic="stddev")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    dist = distance_counterparts("2016-01-01", "2016-01-05", "both", sample).std()
    joined = sample.join(dist)
    assert joined["distance_stddev"].to_dict() == pytest.approx(
        joined["distance"].to_dict()
    )
