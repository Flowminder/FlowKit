# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine_core.utility_queries.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine_core.utility_queries.redacted_joined_spatial_aggregate import (
    RedactedJoinedSpatialAggregate,
)
from flowmachine_core.utility_queries.custom_query import CustomQuery


@pytest.fixture
def redactable_metric():
    return CustomQuery(
        """
    SELECT * FROM (
    VALUES 
    ('a', 0),
    ('b', 0),
    ('c', 0),
    ('d', 0),
    ('e', 0),
    ('f', 0),
    ('g', 0),
    ('h', 0),
    ('i', 0),
    ('j', 0),
    ('k', 0),
    ('l', 0),
    ('m', 0),
    ('n', 0),
    ('o', 0),
    ('p', 0),
    ('q', 0),
    ('r', 0),
    ('s', 0),
    ('t', 0),
    ('u', 0),
    ('v', 0),
    ('w', 0),
    ('x', 0),
    ('y', 0),
    ('z', 0)
    ) as t(subscriber, value)""",
        column_names=["subscriber", "value"],
    )


def test_all_above_threshold(get_dataframe, redactable_locations, redactable_metric):
    """
    Test that values are not returned where there are not enough people in the aggregate.
    """
    in_agg = get_dataframe(
        RedactedJoinedSpatialAggregate(
            joined_spatial_aggregate=JoinedSpatialAggregate(
                locations=redactable_locations, metric=redactable_metric,
            )
        )
    ).pcod
    assert len(in_agg) == 1
    assert list(in_agg)[0] == "a"
