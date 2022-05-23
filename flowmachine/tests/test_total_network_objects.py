# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the quality assurance
Level classes.
"""


import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.features import TotalNetworkObjects, AggregateNetworkObjects


def test_tno_at_lon_lat(get_dataframe):
    """
    Regression test for #108. TNO should work at lon-lat level.
    """
    tno = TotalNetworkObjects(
        start="2016-01-01",
        stop="2016-01-07",
        network_object=make_spatial_unit("versioned-cell"),
        spatial_unit=make_spatial_unit("lon-lat"),
    )
    assert tno.get_dataframe().sum().value == 330


@pytest.mark.parametrize(
    "stat, expected",
    [
        ("avg", 30.541666666666668),
        ("max", 38),
        ("min", 21),
        ("median", 31.0),
        ("mode", 27),
        ("stddev", 4.096437122848253),
        ("variance", 16.780797101449277),
    ],
)
def test_aggregate_returns_correct_values(stat, expected, get_dataframe):
    """
    AggregateNetworkObjects returns correct values.

    """
    instance = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            start="2016-01-01", stop="2016-12-30", table="calls", total_by="hour"
        ),
        statistic=stat,
    )
    df = get_dataframe(instance)

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert pytest.approx(df.value[0]) == expected


def test_count_returns_correct_values(get_dataframe):
    """
    TotalNetworkObjects returns correct values.

    """
    instance = TotalNetworkObjects(
        start="2016-01-01", stop="2016-12-30", table="calls", total_by="hour"
    )
    df = get_dataframe(instance)

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert df.value[34] == 31


def test_bad_total_by():
    """Test value errors are raised for bad 'total_by' param"""
    with pytest.raises(ValueError):
        TotalNetworkObjects(
            start="2016-01-01",
            stop="2016-12-30",
            table="calls",
            total_by="BAD_TOTAL_BY",
        )


@pytest.mark.parametrize(
    "bad_arg, spatial_unit_type",
    [("spatial_unit", "cell"), ("network_object", "lon-lat")],
)
def test_bad_spatial_units(bad_arg, spatial_unit_type):
    """
    Test InvalidSpatialUnitErrors are raised for bad 'network_object' or
    'spatial_unit' params.
    """
    su = make_spatial_unit(spatial_unit_type)
    with pytest.raises(InvalidSpatialUnitError):
        TotalNetworkObjects(
            start="2016-01-01", stop="2016-12-30", table="calls", **{bad_arg: su}
        )


def test_bad_aggregate_by():
    """Test that invalid 'aggregate_by' param raises value error."""
    with pytest.raises(ValueError):
        AggregateNetworkObjects(
            total_network_objects=TotalNetworkObjects(
                start="2016-01-01", stop="2016-12-30", table="calls"
            ),
            aggregate_by="BAD_AGGREGATE_BY",
        )


def test_bad_statistic():
    """Test that invalid stat for aggregate raises value error."""
    with pytest.raises(ValueError):
        AggregateNetworkObjects(
            total_network_objects=TotalNetworkObjects(
                start="2016-01-01", stop="2016-12-30", table="calls"
            ),
            statistic="BAD STAT",
        )


def test_median_returns_correct_values(get_dataframe):
    """
    features.network.TotalNetworkObjects median aggregate returns correct values.

    """
    instance = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            table="calls",
            total_by="hour",
            network_object=make_spatial_unit("versioned-site"),
        ),
        aggregate_by="day",
        statistic="median",
    )

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert get_dataframe(instance).head(1)["value"][0] == 25


def test_mean_returns_correct_values(get_dataframe):
    """
    features.network.TotalNetworkObjects aggregation returns correct values.

    """
    instance = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            start="2016-01-01",
            stop="2016-12-30",
            total_by="hour",
            network_object=make_spatial_unit("versioned-site"),
        ),
        aggregate_by="day",
    )

    #
    #  This will compare the very first
    #  value with an independently
    #  computed value.
    #
    assert get_dataframe(instance).head(1)["value"][0] == pytest.approx(28.7916666666)


@pytest.mark.parametrize(
    "total_by, aggregate_by_expected",
    [
        ("second", "minute"),
        ("minute", "hour"),
        ("hour", "day"),
        ("day", "month"),
        ("month", "year"),
        ("year", "century"),
    ],
)
def test_period_agg_default(total_by, aggregate_by_expected):
    """Correct aggregation period is deduced."""
    inst = AggregateNetworkObjects(
        total_network_objects=TotalNetworkObjects(
            start="2016-01-01", stop="2016-12-30", total_by=total_by
        )
    )
    assert inst.aggregate_by == aggregate_by_expected
