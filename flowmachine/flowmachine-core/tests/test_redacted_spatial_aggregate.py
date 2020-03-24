# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine_core.utility_queries.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine_core.utility_queries.spatial_aggregate import SpatialAggregate


def test_all_above_threshold(get_dataframe, redactable_locations):
    """
    Test that all values in the redacted query are above the redaction threshold.
    """
    df = get_dataframe(
        RedactedSpatialAggregate(
            spatial_aggregate=SpatialAggregate(locations=redactable_locations)
        )
    )
    assert all(df.value > 15)
    assert list(df.pcod) == ["a"]
