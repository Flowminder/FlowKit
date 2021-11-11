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
    TotalLocationEvents() can get activity on a daily level but only above threshold.
    """
    te = RedactedTotalEvents(
        total_events=TotalLocationEvents(
            "2016-01-01",
            "2016-01-02",
            tables=["events.calls"],
            spatial_unit=make_spatial_unit("cell"),
            interval="day",
        )
    )
    us = get_dataframe(
        RedactedUniqueSubscriberCounts(
            unique_subscriber_counts=UniqueSubscriberCounts(
                "2016-01-01", "2016-01-02", tables=["events.calls"]
            )
        )
    )

    te_df = get_dataframe(te)

    assert all(te_df.value > 15)
    assert set(us.location_id) == set(te_df.location_id)
