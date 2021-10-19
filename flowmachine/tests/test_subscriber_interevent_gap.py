# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features.subscriber.interevent_period import *
from flowmachine.features.utilities import EventsTablesUnion

import pytest
import pandas as pd


@pytest.fixture()
def intervent_period(get_dataframe):
    """Returns a dataframe with the subscriber and datetime for each event."""
    postgres_stat_to_pandas_stat = dict(avg="mean", stddev="std", median="median")

    def _intervent_period(*, start, stop, direction, subset, stat):
        events_query = EventsTablesUnion(
            start,
            stop,
            columns=["msisdn", "datetime", "outgoing"],
            subscriber_subset=subset.index.values,
            subscriber_identifier="msisdn",
        )
        events = get_dataframe(events_query)

        if direction == "out":
            events = events[events.outgoing == True]
        elif direction == "in":
            events = events[events.outgoing == False]

        events.sort_values("datetime", inplace=True)
        events = events.assign(duration=events.groupby("subscriber").datetime.diff(+1))
        events = events[["subscriber", "duration"]]

        agg = events.groupby("subscriber").agg(
            lambda x: getattr(x, postgres_stat_to_pandas_stat[stat])()
        )
        agg = (agg["duration"].astype("timedelta64[s]")).to_dict()

        return agg

    return _intervent_period


@pytest.mark.parametrize(
    "start, stop, direction, stat",
    [
        ("2016-01-01", "2016-01-08", "both", "avg"),
        ("2016-01-01", "2016-01-05", "out", "avg"),
        ("2016-01-03", "2016-01-05", "in", "median"),
        ("2016-01-01", "2016-01-08", "both", "stddev"),
    ],
)
def test_interevent_period(
    start, stop, direction, stat, get_dataframe, intervent_period
):
    """
    Test some hand-picked results for IntereventPeriod.
    """

    query = IntereventPeriod(
        start=start,
        stop=stop,
        direction=direction,
        statistic=stat,
        time_resolution="second",
    )
    df = get_dataframe(query).set_index("subscriber")
    sample = df.head(n=5)
    want = intervent_period(
        start=start, stop=stop, direction=direction, stat=stat, subset=sample
    )
    assert query.column_names == ["subscriber", "value"]
    assert (sample["value"]).to_dict() == pytest.approx(want, nan_ok=True)


@pytest.mark.parametrize(
    "start, stop, direction, stat",
    [
        ("2016-01-01", "2016-01-08", "both", "avg"),
        ("2016-01-01", "2016-01-05", "out", "avg"),
        ("2016-01-03", "2016-01-05", "in", "median"),
        ("2016-01-01", "2016-01-08", "both", "stddev"),
    ],
)
def test_interevent_interval(
    start, stop, direction, stat, get_dataframe, intervent_period
):
    """
    Test some hand-picked results for IntereventInterval.
    """

    query = IntereventInterval(
        start=start, stop=stop, direction=direction, statistic=stat
    )
    df = get_dataframe(query).set_index("subscriber")
    sample = df.head(n=5)
    want = intervent_period(
        start=start, stop=stop, direction=direction, stat=stat, subset=sample
    )
    assert query.column_names == ["subscriber", "value"]
    assert (sample["value"].astype("timedelta64[s]")).to_dict() == pytest.approx(
        want, nan_ok=True
    )


@pytest.mark.parametrize("kwarg", ["direction", "statistic"])
def test_interevent_period_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in IntereventPeriod."""

    with pytest.raises(ValueError):
        query = IntereventPeriod("2016-01-03", "2016-01-05", **{kwarg: "error"})
