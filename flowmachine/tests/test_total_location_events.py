# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the spatial activity class
"""
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import TotalLocationEvents


@pytest.mark.usefixtures("skip_datecheck")
@pytest.mark.parametrize("interval", TotalLocationEvents.allowed_intervals)
@pytest.mark.parametrize("direction", ["in", "out", "both"])
def test_total_location_events_column_names(
    exemplar_spatial_unit_param, interval, direction
):
    """Test that column_names property of TotalLocationEvents matches head(0)"""
    tle = TotalLocationEvents(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=exemplar_spatial_unit_param,
        interval=interval,
        direction=direction,
    )
    assert tle.head(0).columns.tolist() == tle.column_names


def test_events_at_cell_level(get_dataframe):
    """
    TotalLocationEvents() returns data at the level of the cell.
    """

    te = TotalLocationEvents(
        "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("cell")
    )
    df = get_dataframe(te)

    # Test one of the values
    df.date = df.date.astype(str)
    val = list(
        df[
            (df.date == "2016-01-03") & (df.location_id == "1Gc6RSfZ") & (df.hour == 17)
        ].value
    )[0]
    assert val == 4


def test_ignore_texts(get_dataframe):
    """
    TotalLocationEvents() can get the total activity at cell level excluding texts.
    """
    te = TotalLocationEvents(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=make_spatial_unit("versioned-site"),
        table="events.calls",
    )
    df = get_dataframe(te)

    # Test one of the values
    df.date = df.date.astype(str)
    val = list(
        df[(df.date == "2016-01-01") & (df.site_id == "0xqNDj") & (df.hour == 3)].value
    )[0]
    assert val == 3


def test_only_incoming(get_dataframe):
    """
    TotalLocationEvents() can get activity, ignoring outgoing calls.
    """
    te = TotalLocationEvents(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=make_spatial_unit("versioned-site"),
        direction="in",
    )
    df = get_dataframe(te)
    # Test one of the values
    df.date = df.date.astype(str)
    val = list(
        df[(df.date == "2016-01-01") & (df.site_id == "6qpN0p") & (df.hour == 0)].value
    )[0]
    assert val == 2


def test_events_daily(get_dataframe):
    """
    TotalLocationEvents() can get activity on a daily level.
    """
    te = TotalLocationEvents(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=make_spatial_unit("versioned-site"),
        interval="day",
    )
    df = get_dataframe(te)

    # Test one of the values
    df.date = df.date.astype(str)
    val = list(df[(df.date == "2016-01-03") & (df.site_id == "B8OaG5")].value)[0]
    assert val == 95


def test_events_min(get_dataframe):
    """
    TotalLocationEvents() can get events on a min-by-min basis.
    """
    te = TotalLocationEvents(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=make_spatial_unit("versioned-site"),
        interval="min",
    )
    df = get_dataframe(te)

    # Test one of the values
    df.date = df.date.astype(str)
    val = list(
        df[
            (df.date == "2016-01-03")
            & (df.site_id == "zdNQx2")
            & (df.hour == 15)
            & (df["min"] == 20)
        ].value
    )[0]
    assert val == 1


def test_bad_direction_raises_error():
    """Total location events raises an error for a bad direction."""
    with pytest.raises(ValueError):
        TotalLocationEvents(
            "2016-01-01",
            "2016-01-04",
            spatial_unit=make_spatial_unit("versioned-site"),
            interval="min",
            direction="BAD_DIRECTION",
        )


def test_bad_interval_raises_error():
    """Total location events raises an error for a bad interval."""
    with pytest.raises(ValueError):
        TotalLocationEvents(
            "2016-01-01",
            "2016-01-04",
            spatial_unit=make_spatial_unit("versioned-site"),
            interval="BAD_INTERVAL",
        )
