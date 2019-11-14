# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import daily_location
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)


def test_all_above_threshold(get_dataframe):
    """
    Test that all values in the redacted query are above the redaction threshold.
    """
    assert all(
        get_dataframe(
            RedactedSpatialAggregate(
                spatial_aggregate=daily_location("2016-01-01").aggregate()
            )
        ).total
        > 15
    )
