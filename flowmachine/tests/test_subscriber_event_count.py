# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.subscriber_event_count import *
from flowmachine.core.errors.flowmachine_errors import MissingDateError
import pytest


def test_count(get_dataframe):
    """
    Test some hand picked periods and tables
    """
    query = SubscriberEventCount("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["DzpZJ2EaVQo2X5vM"].event_count == 46

    query = SubscriberEventCount(
        "2016-01-01",
        "2016-01-08",
        direction="both",
        tables=["events.calls", "events.sms", "events.mds", "events.topups"],
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["DzpZJ2EaVQo2X5vM"].event_count == 69

    query = SubscriberEventCount(
        "2016-01-01", "2016-01-08", direction="both", tables=["events.mds"]
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].event_count == 8

    query = SubscriberEventCount(
        "2016-01-01", "2016-01-08", direction="both", tables="events.mds"
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].event_count == 8

    query = SubscriberEventCount("2016-01-01", "2016-01-08", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].event_count == 24


def test_directed_count_consistent(get_dataframe):
    """
    Test that directed count is consistent.
    """
    out_query = SubscriberEventCount("2016-01-01", "2016-01-08", direction="out")
    out_df = get_dataframe(out_query).set_index("subscriber")

    in_query = SubscriberEventCount("2016-01-01", "2016-01-08", direction="in")
    in_df = get_dataframe(in_query).set_index("subscriber")

    joined = out_df.join(in_df, lsuffix="_out", rsuffix="_in", how="outer")
    joined.loc[~joined.index.isin(out_df.index), "event_count_out"] = 0
    joined.loc[~joined.index.isin(in_df.index), "event_count_in"] = 0

    joined["event_count_got"] = joined.sum(axis=1)

    both_query = SubscriberEventCount("2016-01-01", "2016-01-08", direction="both")
    both_df = get_dataframe(both_query).set_index("subscriber")

    joined = joined.join(both_df, how="outer")

    assert all(joined["event_count_got"] == joined["event_count"])


def test_directed_count_undirected_tables_raises():
    """
    Test that requesting directed counts of undirected tables raises warning and errors.
    """
    with pytest.warns(UserWarning):
        query = SubscriberEventCount(
            "2016-01-01",
            "2016-01-08",
            direction="out",
            tables=["events.calls", "events.mds"],
        )

    with pytest.raises(MissingDateError):
        query = SubscriberEventCount(
            "2016-01-01", "2016-01-08", direction="out", tables=["events.mds"]
        )


def test_directed_count_skips_undirected_tables(get_dataframe):
    """
    Test that directed count skips undirected tables.
    """
    want_query = SubscriberEventCount(
        "2016-01-01", "2016-01-08", direction="out", tables=["events.calls"]
    )
    want_df = get_dataframe(want_query).set_index("subscriber")

    got_query = SubscriberEventCount(
        "2016-01-01",
        "2016-01-08",
        direction="out",
        tables=["events.calls", "events.mds"],
    )
    got_df = get_dataframe(got_query).set_index("subscriber")

    assert all(want_df.event_count == got_df.event_count)
