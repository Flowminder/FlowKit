# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import RadiusOfGyration, daily_location
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.redacted_joined_spatial_aggregate import (
    RedactedJoinedSpatialAggregate,
)


def test_all_above_threshold(get_dataframe):
    """
    Test that values are not returned where there are not enough people in the aggregate.
    """
    in_agg = get_dataframe(
        RedactedJoinedSpatialAggregate(
            joined_spatial_aggregate=JoinedSpatialAggregate(
                locations=daily_location("2016-01-01"),
                metric=RadiusOfGyration("2016-01-01", "2016-01-02"),
            )
        )
    ).pcod
    assert len(in_agg) > 0
    under_15 = get_dataframe(
        daily_location("2016-01-01")
        .aggregate()
        .numeric_subset(col="value", low=0, high=15)
    ).pcod
    assert set(under_15).isdisjoint(in_agg)
