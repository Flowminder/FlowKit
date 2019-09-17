# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

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


@pytest.mark.parametrize("identifier", ("msisdn", "imei", "imsi"))
def test_colums_are_set(identifier):
    """Test that all the columns and identifier are set in the SQL."""
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )
    columns = ss.head(0).columns

    assert identifier in columns
    assert "timestamp" in columns
    assert "cell_id" in columns


def test_error_on_start_is_stop(get_dataframe):
    """Test that a value error is raised when start == stop"""
    with pytest.raises(ValueError):
        SubscriberSigntings("2016-01-01", "2016-01-01")
