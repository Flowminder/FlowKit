# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.subscriber_call_durations import *
import pytest


@pytest.mark.parametrize(
    "query",
    [
        SubscriberCallDurations,
        PairedPerLocationSubscriberCallDurations,
        PerLocationSubscriberCallDurations,
        PairedSubscriberCallDurations,
    ],
)
@pytest.mark.parametrize("stat", valid_stats)
def test_subscriber_call_durations_column_names(query, exemplar_level_param, stat):
    """
    Test that column_names property matches head(0)
    """
    query_instance = query(
        "2016-01-01", "2016-01-07", **exemplar_level_param, statistic=stat
    )
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


def test_polygon_tables(get_dataframe):
    """
    Test that custom polygons can be used.
    """
    per_location_durations = PerLocationSubscriberCallDurations(
        "2016-01-01",
        "2016-01-07",
        level="polygon",
        polygon_table="geography.admin3",
        column_name="admin3name",
    )
    df = get_dataframe(per_location_durations)

    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    df = get_dataframe(
        PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    )
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 24086
    df = get_dataframe(
        PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="both")
    )
    assert (
        df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum
        == 24086 + 12281
    )

    paired_per_location_durations = PairedPerLocationSubscriberCallDurations(
        "2016-01-01",
        "2016-01-07",
        level="polygon",
        polygon_table="geography.admin3",
        column_name="admin3name",
    )

    df = get_dataframe(paired_per_location_durations)
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    assert (
        df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum
        == 24086
    )


def test_durations(get_dataframe):
    """
    Test some hand picked durations
    """
    out_durations = SubscriberCallDurations("2016-01-01", "2016-01-07")
    df = get_dataframe(out_durations).set_index("subscriber")
    assert df.loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    df = get_dataframe(
        SubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    ).set_index("subscriber")
    assert df.loc["nL9KYGXpz2G5mvDa"].duration_sum == 24086
    df = get_dataframe(
        SubscriberCallDurations("2016-01-01", "2016-01-07", direction="both")
    ).set_index("subscriber")
    assert df.loc["nL9KYGXpz2G5mvDa"].duration_sum == 24086 + 12281


def test_paired_durations(get_dataframe):
    """
    Test paired durations sum to the same as in/out durations
    """
    paired_durations = PairedSubscriberCallDurations("2016-01-01", "2016-01-07")
    df = get_dataframe(paired_durations)
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    assert (
        df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum
        == 24086
    )


def test_per_location_durations(get_dataframe):
    """
    Test per location durations sums to the same as in/out durations
    """
    per_location_durations = PerLocationSubscriberCallDurations(
        "2016-01-01", "2016-01-07"
    )
    df = get_dataframe(per_location_durations)
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    df = get_dataframe(
        PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    )
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 24086
    df = get_dataframe(
        PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="both")
    )
    assert (
        df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum
        == 24086 + 12281
    )


def test_paired_per_location_durations(get_dataframe):
    """
    Test paired per location durations sum to the same as in/out durations
    """
    paired_per_location_durations = PairedPerLocationSubscriberCallDurations(
        "2016-01-01", "2016-01-07"
    )
    df = get_dataframe(paired_per_location_durations)
    assert df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum == 12281
    assert (
        df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum
        == 24086
    )


def test_direction_checks():
    """
    Test that bad direction params are rejected
    """
    with pytest.raises(ValueError):
        PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="alf")
    with pytest.raises(ValueError):
        SubscriberCallDurations("2016-01-01", "2016-01-07", direction="mooses")


def test_long_runtime_warning():
    """
    Test that a warning about potentially long runtime is raised.
    """
    with pytest.warns(UserWarning):
        PairedPerLocationSubscriberCallDurations("2016-01-01", "2016-01-07")
