# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.event_count import *

import pytest


def test_event_count(get_dataframe):
    """
    Test some hand picked periods and tables
    """
    query = EventCount("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["DzpZJ2EaVQo2X5vM"].value == 46

    query = EventCount(
        "2016-01-01",
        "2016-01-08",
        direction="both",
        tables=["events.calls", "events.sms", "events.mds", "events.topups"],
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["DzpZJ2EaVQo2X5vM"].value == 69

    query = EventCount(
        "2016-01-01", "2016-01-08", direction="both", tables=["events.mds"]
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].value == 8

    query = EventCount(
        "2016-01-01", "2016-01-08", direction="both", tables="events.mds"
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].value == 8

    query = EventCount("2016-01-01", "2016-01-08", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["E0LZAa7AyNd34Djq"].value == 24

    query = EventCount("2016-01-01", "2016-01-08", direction="in")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["4dqenN2oQZExwEK2"].value == 12


@pytest.mark.parametrize("kwarg", ["direction"])
def test_event_count_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in EventCount."""

    with pytest.raises(ValueError):
        query = EventCount("2016-01-03", "2016-01-05", **{kwarg: "error"})


def test_directed_count_consistent(get_dataframe):
    """
    Test that directed count is consistent.
    """
    out_query = EventCount("2016-01-01", "2016-01-08", direction="out")
    out_df = get_dataframe(out_query).set_index("subscriber")

    in_query = EventCount("2016-01-01", "2016-01-08", direction="in")
    in_df = get_dataframe(in_query).set_index("subscriber")

    joined = out_df.join(in_df, lsuffix="_out", rsuffix="_in", how="outer")
    joined.loc[~joined.index.isin(out_df.index), "value_out"] = 0
    joined.loc[~joined.index.isin(in_df.index), "value_in"] = 0

    joined["value"] = joined.sum(axis=1)

    both_query = EventCount("2016-01-01", "2016-01-08", direction="both")
    both_df = get_dataframe(both_query).set_index("subscriber")

    assert joined["value"].to_dict() == both_df["value"].to_dict()


def test_directed_count_undirected_tables_raises():
    """
    Test that requesting directed counts of undirected tables raises warning and errors.
    """
    with pytest.raises(ValueError):
        query = EventCount(
            "2016-01-01", "2016-01-08", direction="out", tables=["events.mds"]
        )
