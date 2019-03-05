# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.interevent_period import *

import pytest
import pandas as pd


@pytest.fixture()
def intervent_period(get_dataframe):
    """ Returns a dataframe with the subscriber and datetime for each event. """

    def _intervent_period(start, stop, direction, subset, stat):
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

        agg = events.groupby("subscriber").agg(lambda x: getattr(x, stat)())
        agg = pd.to_numeric(agg["duration"]).to_dict()

        return agg

    return _intervent_period


def test_interevent_period(get_dataframe, intervent_period):
    """
    Test some hand-picked results for IntereventPeriod.
    """

    query = IntereventPeriod("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    want = intervent_period("2016-01-01", "2016-01-08", "both", sample, "mean")
    assert query.column_names == ["subscriber", "value"]
    assert pd.to_numeric(sample["value"]).to_dict() == pytest.approx(want)

    query = IntereventPeriod("2016-01-01", "2016-01-05", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    want = intervent_period("2016-01-01", "2016-01-05", "out", sample, "mean")
    assert query.column_names == ["subscriber", "value"]
    assert pd.to_numeric(sample["value"]).to_dict() == pytest.approx(want)

    query = IntereventPeriod("2016-01-03", "2016-01-05", direction="in")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    want = intervent_period("2016-01-03", "2016-01-05", "in", sample, "mean")
    assert query.column_names == ["subscriber", "value"]
    assert pd.to_numeric(sample["value"]).to_dict() == pytest.approx(want)

    query = IntereventPeriod("2016-01-01", "2016-01-08", "stddev")
    df = get_dataframe(query).set_index("subscriber")
    sample = df.sample(n=5)
    want = intervent_period("2016-01-01", "2016-01-08", "both", sample, "std")
    assert query.column_names == ["subscriber", "value"]
    assert pd.to_numeric(sample["value"]).to_dict() == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["direction", "statistic"])
def test_interevent_period_errors(kwarg):
    """ Test ValueError is raised for non-compliant kwarg in IntereventPeriod.
    """

    with pytest.raises(ValueError):
        query = IntereventPeriod("2016-01-03", "2016-01-05", **{kwarg: "error"})
