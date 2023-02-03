# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for CallDays Class, CallDays counts numbers of distinct
days where an subscriber has made at least one call
"""


import pytest


from flowmachine.core import make_spatial_unit
from flowmachine.features import CallDays, SubscriberLocations
import numpy as np


@pytest.mark.usefixtures("skip_datecheck")
def test_calldays_column_names(exemplar_spatial_unit_param):
    """Test that CallDays column_names property is correct"""
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-03", spatial_unit=exemplar_spatial_unit_param
        )
    )
    assert cd.head(0).columns.tolist() == cd.column_names


def test_call_days_returns_expected_counts_per_subscriber(get_dataframe):
    """
    Test that the returned data is correct for a given subscriber.
    """
    test_values = (
        ("Z89mWDgZrr3qpnlB", "2016-01-01", "2016-01-03", 9),
        ("Z89mWDgZrr3qpnlB", "2016-01-01", "2016-01-08", 30),
        ("038OVABN11Ak4W5P", "2016-01-01", "2016-01-03", 6),
        ("038OVABN11Ak4W5P", "2016-01-01", "2016-01-08", 32),
    )
    for subscriber, start, end, calls in test_values:
        cd = CallDays(
            SubscriberLocations(
                start, end, spatial_unit=make_spatial_unit("versioned-site")
            )
        )
        df = get_dataframe(cd).query('subscriber == "{}"'.format(subscriber))
        assert df.value.sum() == calls


def test_call_days_returns_expected_counts_per_subscriber_tower(get_dataframe):
    """
    Test that the returned data is correct for a given subscriber-location pair.
    """
    test_values = (
        ("Z89mWDgZrr3qpnlB", "m9jL23", "2016-01-01", "2016-01-03", 2),
        ("Z89mWDgZrr3qpnlB", "qvkp6J", "2016-01-01", "2016-01-08", 4),
        ("038OVABN11Ak4W5P", "QeBRM8", "2016-01-01", "2016-01-03", 1),
        ("038OVABN11Ak4W5P", "nWM8R3", "2016-01-01", "2016-01-08", 5),
    )
    for subscriber, location, start, end, calls in test_values:
        cd = CallDays(
            SubscriberLocations(
                start, end, spatial_unit=make_spatial_unit("versioned-site")
            )
        )
        df = get_dataframe(cd).query(
            'subscriber == "{}" & site_id == "{}"'.format(subscriber, location)
        )
        assert df.value.values[0] == calls


def test_locations_are_only_repeated_once_per_subscriber(get_dataframe):
    """
    Test that each location occurs only once per subscriber.
    """

    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        )
    )
    df = get_dataframe(cd)
    assert not np.any(df.groupby(["subscriber", "location_id"]).count() > 1)
