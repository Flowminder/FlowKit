# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import CustomQuery, make_spatial_unit
from flowmachine.features import (
    DistanceSeries,
    ImputedDistanceSeries,
    IterativeMedianFilter,
    SubscriberLocations,
)


@pytest.mark.parametrize(
    "partition_column, order_column, expected",
    [
        (None, None, [1, 1, 1, 1.1, 1.1, 1.95, 2, 2, 2.05, 2.99, 3, 3, 3]),
        ("part", None, [1, 1, 0.9, 0.95, 0.95, 0.95, 1.95, 2, 2, 2.99, 3, 3, 3]),
        ("part", "ord", [0.9, 1, 1, 0.95, 0.95, 0.95, 2, 2, 1.95, 3, 3, 3, 2.99]),
        (None, "ord", [2.1, 2.1, 2.05, 2, 2, 2, 1.95, 1.1, 1.1, 1.1, 1.1, 2.99, 2.99]),
    ],
)
def test_partition_and_order_can_be_ommitted(
    partition_column, order_column, expected, get_dataframe
):
    """
    Test that the filter can be applied without partitioning or ordering.
    """
    test_query = CustomQuery(
        """SELECT * FROM (VALUES 
            (1, 1, 3), (1.1, 1, 2), (0.9, 1, 1), (1.1, 2, 3), 
            (0.95, 2, 2), (2.1, 2, 1), (1.95, 3, 3), (2.0, 3, 2),
             (2.05, 3, 1), (3.11, 4, 4), (2.99, 4, 3), (3.05, 4, 2), 
             (3.0, 4, 1)) as t(value, part, ord)""",
        column_names=["value", "part", "ord"],
    )
    smoothed = get_dataframe(
        IterativeMedianFilter(
            query_to_filter=test_query,
            filter_window_size=3,
            partition_column=partition_column,
            order_column=order_column,
        )
    )
    assert smoothed.value.tolist() == expected


@pytest.mark.parametrize(
    "column_arg", ["column_to_filter", "partition_column", "order_column"]
)
def test_column_must_exist(column_arg):
    """
    Check errors for required columns.
    """
    with pytest.raises(ValueError, match=column_arg):
        sl = SubscriberLocations(
            "2016-01-01",
            "2016-01-07",
            spatial_unit=make_spatial_unit("lon-lat"),
            hours=(20, 0),
        )
        ds = DistanceSeries(subscriber_locations=sl, statistic="min")
        IterativeMedianFilter(
            query_to_filter=ImputedDistanceSeries(distance_series=ds),
            filter_window_size=3,
            **{column_arg: "NOT_A_VALID_COLUMN"},
        )


@pytest.mark.parametrize(
    "size, match",
    [
        (1, "filter_window_size must be odd and greater than 1"),
        (0, "filter_window_size must be odd and greater than 1"),
        (-1, "filter_window_size must be odd and greater than 1"),
        (4, "filter_window_size must be odd"),
    ],
)
def test_bad_window(size, match):
    """
    Test some median unfriendly window sizes raise errors.
    """
    with pytest.raises(ValueError, match=match):
        sl = SubscriberLocations(
            "2016-01-01",
            "2016-01-07",
            spatial_unit=make_spatial_unit("lon-lat"),
            hours=(20, 0),
        )
        ds = DistanceSeries(subscriber_locations=sl, statistic="min")
        IterativeMedianFilter(
            query_to_filter=ImputedDistanceSeries(distance_series=ds),
            filter_window_size=size,
        )


def test_smooth(get_dataframe):
    """
    Test that iterated median filter matches an independently calculated result.
    """
    sl = SubscriberLocations(
        "2016-01-01",
        "2016-01-07",
        spatial_unit=make_spatial_unit("lon-lat"),
        hours=(20, 0),
    )
    ds = DistanceSeries(subscriber_locations=sl, statistic="min")
    smoothed_df = get_dataframe(
        IterativeMedianFilter(
            query_to_filter=ImputedDistanceSeries(distance_series=ds),
            filter_window_size=3,
        )
    )
    assert smoothed_df.set_index("subscriber").loc[
        "038OVABN11Ak4W5P"
    ].value.tolist() == pytest.approx(
        [
            9343367.56611,
            9343367.56611,
            9343367.56611,
            9343367.56611,
            9343367.56611,
            9221492.17419,
        ]
    )
