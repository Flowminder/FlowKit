# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import pytz
from datetime import datetime

from flowmachine.features import SubscriberSigntings


def test_main_colums_are_set():
    """Test that all the columns and identifier are set on the object."""
    identifier = "imei"
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    assert identifier in ss.column_names
    assert "timestamp" in ss.column_names
    assert "cell_id" in ss.column_names


def test_subscriber_identifier_can_be_uppercase():
    """Test that subscriber_identifier can be provided in uppercase."""
    identifier = "IMEI"
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    assert identifier.lower() in ss.column_names


def test_msisdn_set_as_identifier():
    """Test that msisdn is set as the default identifier."""
    ss = SubscriberSigntings("2016-01-01", "2016-01-02")

    assert "msisdn" in ss.column_names


@pytest.mark.parametrize("identifier", ("msisdn", "imei", "imsi"))
def test_colums_are_set_in_sql(identifier):
    """Test that all the columns and identifier are set in the SQL."""
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )
    columns = ss.head(0).columns

    assert identifier in columns
    assert "timestamp" in columns
    assert "cell_id" in columns


def test_error_on_start_is_stop():
    """Test that a value error is raised when start == stop"""
    with pytest.raises(ValueError):
        SubscriberSigntings("2016-01-01", "2016-01-01")


def test_set_date_none():
    """Test that setting a start/stop to None will use min/max dates."""
    ss = SubscriberSigntings(None, "2016-01-04")
    df = ss.head()

    min = df["timestamp"].min().to_pydatetime()
    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 0, 0, 0))

    assert min.timestamp() > min_comparison.timestamp()

    ss = SubscriberSigntings("2016-01-01", None)
    df = ss.head()

    max = df["timestamp"].max().to_pydatetime()
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 10, 0, 0, 0))

    assert max.timestamp() < max_comparison.timestamp()


def test_dates_select_correct_data():
    """Test that dates select the correct data."""
    ss = SubscriberSigntings("2016-01-01", "2016-01-02")
    df = ss.head(50)

    min = df["timestamp"].min().to_pydatetime()
    max = df["timestamp"].max().to_pydatetime()
    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 0, 0, 0))
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 10, 0, 0, 0))

    assert min.timestamp() > min_comparison.timestamp()
    assert max.timestamp() < max_comparison.timestamp()
