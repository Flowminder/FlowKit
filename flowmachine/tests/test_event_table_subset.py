# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for EventTableSubset
"""
import pytest
import pytz

from datetime import datetime

from flowmachine.core.errors import MissingDateError
from flowmachine.core.errors.flowmachine_errors import PreFlightFailedException
from flowmachine.features.utilities.event_table_subset import EventTableSubset


def test_error_on_start_is_stop(get_dataframe):
    """Test that a value error is raised when start == stop"""
    with pytest.raises(ValueError):
        EventTableSubset(start="2016-01-01", stop="2016-01-01")


def test_handles_dates(get_dataframe):
    """
    Date subsetter can handle timestamp without hours or mins.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    df = get_dataframe(sd)

    minimum = df["datetime"].min().to_pydatetime()
    maximum = df["datetime"].max().to_pydatetime()

    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1))
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 2))

    assert minimum.timestamp() > min_comparison.timestamp()
    assert maximum.timestamp() < max_comparison.timestamp()


@pytest.mark.check_available_dates
def test_warns_on_missing():
    """
    Date subsetter should warn on missing dates.
    """
    message = "115 of 122 calendar dates missing. Earliest date is 2016-01-01, latest is 2016-01-07"
    with pytest.warns(UserWarning, match=message):
        EventTableSubset(start="2016-01-01", stop="2016-05-02").preflight()


@pytest.mark.check_available_dates
def test_error_on_all_missing():
    """
    Date subsetter should error when all dates are missing.
    """
    with pytest.raises(PreFlightFailedException) as exc:
        EventTableSubset(start="2016-05-01", stop="2016-05-02").preflight()


def test_handles_mins(get_dataframe):
    """
    Date subsetter can handle timestamps including the times.
    """
    sd = EventTableSubset(start="2016-01-01 13:30:30", stop="2016-01-02 16:25:00")
    df = get_dataframe(sd)

    minimum = df["datetime"].min().to_pydatetime()
    maximum = df["datetime"].max().to_pydatetime()

    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 13, 30, 30))
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 2, 16, 25, 0))

    assert minimum.timestamp() > min_comparison.timestamp()
    assert maximum.timestamp() < max_comparison.timestamp()


def test_head_has_column_names(get_dataframe):
    """
    Returning the dataframe gives the expected column names.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    assert [
        "country_code",
        "datetime",
        "duration",
        "id",
        "imei",
        "imsi",
        "location_id",
        "subscriber",
        "msisdn_counterpart",
        "network",
        "operator_code",
        "outgoing",
        "tac",
    ] == get_dataframe(sd).columns.tolist()


def test_can_subset_by_hour(get_dataframe):
    """
    EventTableSubset can subset by a range of hours
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-04", hours=(12, 17))
    df = get_dataframe(sd)
    df["hour"] = df.datetime.apply(lambda x: x.hour)
    df["day"] = df.datetime.apply(lambda x: x.day)
    Range = df.hour.max() - df.hour.min()
    assert 4 == Range
    # Also check that all the dates are still there
    assert 3 in df.day
    assert 2 in df.day
    assert 1 in df.day


def test_handles_backwards_hours(get_dataframe):
    """
    If the subscriber passes hours that are 'backwards' this will be interpreted as spanning midnight.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-04", hours=(20, 5))
    df = get_dataframe(sd)
    df["hour"] = df.datetime.apply(lambda x: x.hour)
    df["day"] = df.datetime.apply(lambda x: x.day)
    unique_hours = list(df.hour.unique())
    unique_hours.sort()
    assert [0, 1, 2, 3, 4, 20, 21, 22, 23] == unique_hours
    # Also check that all the dates are still there
    assert 3 in df.day
    assert 2 in df.day
    assert 1 in df.day


def test_default_dates(get_dataframe):
    """
    Test whether not passing a start and/or stop date will
    default to the min and/or max dates in the table.
    """
    sd = EventTableSubset(start=None, stop="2016-01-04")
    df = get_dataframe(sd)

    minimum = df["datetime"].min().to_pydatetime()
    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 0, 0, 0))
    assert minimum.timestamp() > min_comparison.timestamp()

    sd = EventTableSubset(start="2016-01-04", stop=None, hours=(20, 5))
    df = get_dataframe(sd)

    maximum = df["datetime"].max().to_pydatetime()
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 8, 0, 0, 0))
    assert maximum.timestamp() < max_comparison.timestamp()


def test_explain(get_dataframe):
    """
    EventTableSubset.explain() method returns a string
    """

    # Usually not a critical function, so let's simply test by
    # asserting that it returns a string
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    explain_string = sd.explain()
    assert isinstance(explain_string, str)
    assert isinstance(sd.explain(analyse=True), str)


def test_avoids_searching_extra_tables(get_dataframe):
    """
    EventTableSubset query doesn't look in additional partitioned tables.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    explain_string = sd.explain()
    assert "calls_20160103" not in explain_string
