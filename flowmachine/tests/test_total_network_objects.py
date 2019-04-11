# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the quality assurance
Level classes.
"""


import pytest

import flowmachine.features.network as network
from flowmachine.features import TotalNetworkObjects, AggregateNetworkObjects


def test_count_returns_correct_values(get_dataframe):
    """
    TotalNetworkObjects returns correct values.

    """
    instance = network.TotalNetworkObjects(
        start="2016-01-01", stop="2016-12-30", table="calls", period="hour"
    )
    df = get_dataframe(instance)

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert df.total[34] == 31


@pytest.mark.parametrize(
    "bad_arg, bad_val",
    [("period", "BAD_PERIOD"), ("level", "cell"), ("network_object", "BAD_OBJECT")],
)
def test_bad_params(bad_arg, bad_val):
    """Test value errors are raised for bad params"""
    with pytest.raises(ValueError):
        TotalNetworkObjects(
            start="2016-01-01", stop="2016-12-30", table="calls", **{bad_arg: bad_val}
        )


def test_bad_statistic():
    """Test that invalid stat for aggregate raises value error."""
    with pytest.raises(ValueError):
        AggregateNetworkObjects(
            total_network_objects=TotalNetworkObjects(
                start="2016-01-01", stop="2016-12-30", table="calls"
            ),
            statistic="count",
        )


def test_median_returns_correct_values(get_dataframe):
    """
    features.network.TotalNetworkObjects median aggregate returns correct values.

    """
    instance = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            table="calls", period="hour", network_object="versioned-site"
        ),
        by="day",
        statistic="median",
    )

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert get_dataframe(instance).head(1)["median"][0] == 25


def test_mean_returns_correct_values(get_dataframe):
    """
    features.network.TotalNetworkObjects aggregation returns correct values.

    """
    instance = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            start="2016-01-01",
            stop="2016-12-30",
            period="hour",
            network_object="versioned-site",
        ),
        by="day",
    )

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert get_dataframe(instance).head(1)["avg"][0] == pytest.approx(28.7916666666)


@pytest.mark.parametrize(
    "period, expected",
    [
        ("second", "minute"),
        ("minute", "hour"),
        ("hour", "day"),
        ("day", "month"),
        ("month", "year"),
        ("year", "century"),
    ],
)
def test_period_agg_default(period, expected):
    """Correct aggregation period is deduced."""
    inst = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            start="2016-01-01", stop="2016-12-30", period=period
        )
    )
    assert inst.by == expected
