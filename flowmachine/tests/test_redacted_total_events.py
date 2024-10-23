# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    TotalLocationEvents,
    UniqueSubscriberCounts,
)

from flowmachine.features.location.redacted_total_events import RedactedTotalEvents
from flowmachine.features.location.redacted_unique_subscriber_counts import (
    RedactedUniqueSubscriberCounts,
)


def test_all_above_threshold(get_dataframe):
    """
    RedactedTotalEvents() can get activity on a daily level but only above threshold.
    """
    te = RedactedTotalEvents(
        total_events=TotalLocationEvents(
            "2016-01-01",
            "2016-01-02",
            spatial_unit=make_spatial_unit("cell"),
            interval="day",
            table=["calls"],
        )
    )
    us = get_dataframe(
        RedactedUniqueSubscriberCounts(
            unique_subscriber_counts=UniqueSubscriberCounts(
                "2016-01-01", "2016-01-02", table=["calls"]
            )
        )
    )

    te_df = get_dataframe(te)
    assert all(te_df.value > 15)
    assert set(te_df.location_id) == set(us.location_id)


def test_all_above_threshold_hour_bucket(get_dataframe):
    """
    RedactedTotalEvents() can get activity on an hourly level but only above threshold.
    """
    te = RedactedTotalEvents(
        total_events=TotalLocationEvents(
            "2016-01-01",
            "2016-01-02",
            spatial_unit=make_spatial_unit("cell"),
            interval="hour",
            table=["events.calls"],
        )
    )

    te_df = get_dataframe(te)
    assert all(te_df.value > 15)


def test_all_above_threshold_minute_bucket(get_dataframe):
    """
    RedactedTotalEvents() can get activity on a minute level but only above threshold.
    """
    te = RedactedTotalEvents(
        total_events=TotalLocationEvents(
            "2016-01-01 12:00",
            "2016-01-01 13:00",
            spatial_unit=make_spatial_unit("cell"),
            interval="min",
            table=["events.calls"],
        )
    )

    te_df = get_dataframe(te)
    assert all(te_df.value > 15)
