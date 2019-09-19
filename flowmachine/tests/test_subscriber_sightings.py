# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import pytz
from datetime import datetime

from flowmachine.features import SubscriberSigntings
from flowmachine.core import CustomQuery
from flowmachine.core.subscriber_subsetter import SubscriberSubsetterForFlowmachineQuery

def test_columns_and_identifier_are_set():
    """Test that all the columns and identifier are set on the object."""
    identifier = "imei"
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    assert identifier == ss.subscriber_identifier
    assert "subscriber" in ss.column_names
    assert "datetime" in ss.column_names
    assert "location_id" in ss.column_names


def test_subscriber_identifier_can_be_uppercase():
    """Test that subscriber_identifier can be provided in uppercase."""
    identifier = "IMEI"
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    assert identifier.lower() == ss.subscriber_identifier


def test_msisdn_set_as_identifier():
    """Test that msisdn is set as the default identifier."""
    ss = SubscriberSigntings("2016-01-01", "2016-01-02")

    assert "msisdn" == ss.subscriber_identifier


@pytest.mark.parametrize("identifier", ("msisdn", "imei", "imsi"))
def test_colums_are_set_in_sql(identifier):
    """Test that the identifier is set an the colums."""
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    assert f"{identifier} AS subscriber" in ss.columns


def test_error_on_start_is_stop():
    """Test that a value error is raised when start == stop"""
    with pytest.raises(ValueError):
        SubscriberSigntings("2016-01-01", "2016-01-01")


def test_set_date_none():
    """Test that setting a start/stop to None will use min/max dates."""
    ss = SubscriberSigntings(None, "2016-01-04")
    df = ss.head()

    min = df["datetime"].min().to_pydatetime()
    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 0, 0, 0))

    assert min.timestamp() > min_comparison.timestamp()

    ss = SubscriberSigntings("2016-01-01", None)
    df = ss.head()

    max = df["datetime"].max().to_pydatetime()
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 10, 0, 0, 0))

    assert max.timestamp() < max_comparison.timestamp()


def test_dates_select_correct_data():
    """Test that dates select the correct data."""
    ss = SubscriberSigntings("2016-01-01", "2016-01-02")
    df = ss.head(50)

    min = df["datetime"].min().to_pydatetime()
    max = df["datetime"].max().to_pydatetime()
    min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1, 0, 0, 0))
    max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 10, 0, 0, 0))

    assert min.timestamp() > min_comparison.timestamp()
    assert max.timestamp() < max_comparison.timestamp()

def test_that_subscriber_subset_is_added(get_dataframe):
    """Test that subscriber_subset is added to the output SQL."""
    subsetter = SubscriberSubsetterForFlowmachineQuery(CustomQuery(
        "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 400",
        ["subscriber"],
    ))
    
    ss = SubscriberSigntings("2016-01-01", "2016-01-02", subscriber_subset=subsetter)
    query = ss.get_query()
    
    assert "FROM events.calls" in query
    assert "WHERE duration < 400" in query
    assert "tbl.subscriber = subset_query.subscriber" in query
