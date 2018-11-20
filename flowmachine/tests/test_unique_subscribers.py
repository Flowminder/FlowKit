# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import UniqueSubscribers


def test_unique_subscriber_column_names(get_dataframe):
    """Test that column_names property of UniqueSubscribers is accurate"""
    us = UniqueSubscribers("2016-01-01", "2016-01-02")
    assert get_dataframe(us).columns.tolist() == us.column_names


def test_returns_set(get_dataframe):
    """
    UniqueSubscribers() returns set.
    """
    UU = UniqueSubscribers("2016-01-01", "2016-01-02")
    u_set = UU.as_set()
    assert isinstance(u_set, set)
    assert len(u_set) == len(get_dataframe(UU))


def test_subscribers_unique(get_dataframe):
    """
    Returned dataframe has unique subscribers.
    """
    UU = UniqueSubscribers("2016-01-01", "2016-01-02")
    assert get_dataframe(UU)["subscriber"].is_unique
