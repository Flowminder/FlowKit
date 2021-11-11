# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import UniqueSubscriberCounts
from flowmachine.features.location.redacted_unique_subscriber_counts import (
    RedactedUniqueSubscriberCounts,
)


def test_all_above_threshold(get_dataframe):
    """
    Test that all values in the redacted query are above the redaction threshold.
    """
    us = UniqueSubscriberCounts("2016-01-01", "2016-01-02", tables=["events.calls"])
    rus_df = get_dataframe(RedactedUniqueSubscriberCounts(unique_subscriber_counts=us))
    us_df = get_dataframe(us)
    assert all(rus_df.value > 15)
    assert set(us_df.location_id).issuperset(set(rus_df.location_id))
    assert set(us_df.location_id) != set(rus_df.location_id)
