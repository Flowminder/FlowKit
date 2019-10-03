# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import date

import pytest
from flowmachine.features import DistanceSeries, daily_location, SubscriberLocations
from numpy import isnan

from flowmachine.core import make_spatial_unit
from unittest.mock import Mock


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("min", 0, 0),
        ("median", 426.73295117925, 407.89567243203),
        ("stddev", 259.637731932506, 193.169444586714),
        ("avg", 298.78017265186, 375.960833781832),
        ("sum", 896.34051795558, 4511.53000538199),
        ("variance", 67411.7518430558, 37314.4343219396),
    ],
)
def test_returns_expected_values(stat, sub_a_expected, sub_b_expected, get_dataframe):
    """
    Test that we get expected return values for the various statistics
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location=rl,
            statistic=stat,
        )
    ).set_index(["subscriber", "datetime"])
    assert df.loc[(sub_a_id, date(2016, 1, 1))].value == pytest.approx(sub_a_expected)
    assert df.loc[(sub_b_id, date(2016, 1, 6))].value == pytest.approx(sub_b_expected)


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("min", 9284030.27181416, 9123237.1676943),
        ("median", 9327090.49789517, 9348965.25483016),
        ("stddev", 213651.966918163, 180809.405030127),
        ("avg", 9428284.48898054, 9390833.62618702),
        ("sum", 28284853.4669416, 112690003.514244),
        ("variance", 45647162968.0, 32692040947.3485),
    ],
)
def test_returns_expected_values(stat, sub_a_expected, sub_b_expected, get_dataframe):
    """
    Test that we get expected return values for the various statistics with 0, 0 reference
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            statistic=stat,
        )
    ).set_index(["subscriber", "datetime"])
    assert df.loc[(sub_a_id, date(2016, 1, 1))].value == pytest.approx(sub_a_expected)
    assert df.loc[(sub_b_id, date(2016, 1, 6))].value == pytest.approx(sub_b_expected)


def test_error_when_subs_locations_not_point_geom():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    rl = daily_location("2016-01-01")

    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("admin3")
            )
        )


def test_error_on_spatial_unit_mismatch():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    rl = daily_location("2016-01-01")

    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location=rl,
        )


def test_invalid_statistic_raises_error():
    """
    Test that passing an invalid statistic raises an error.
    """
    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            statistic="NOT_A_STATISTIC",
        )
