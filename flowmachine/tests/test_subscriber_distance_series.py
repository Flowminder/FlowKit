# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import datetime

import pytest
from flowmachine.features import DistanceSeries, daily_location, SubscriberLocations

from flowmachine.core import make_spatial_unit
import pandas as pd


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
    )
    df = (
        df.assign(datetime=pd.to_datetime(df.datetime))
        .set_index(["subscriber", "datetime"])
        .sort_index()
    )
    sub = df.loc[sub_a_id]
    assert df.loc[sub_a_id].loc["2016-01-01"].value == pytest.approx(sub_a_expected)
    assert df.loc[(sub_b_id, datetime(2016, 1, 6))].value == pytest.approx(
        sub_b_expected
    )


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
def test_returns_expected_values_fixed_point(
    stat, sub_a_expected, sub_b_expected, get_dataframe
):
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
    )
    df = df.assign(datetime=pd.to_datetime(df.datetime)).set_index(
        ["subscriber", "datetime"]
    )
    assert df.loc[(sub_a_id, datetime(2016, 1, 1))].value == pytest.approx(
        sub_a_expected
    )
    assert df.loc[(sub_b_id, datetime(2016, 1, 6))].value == pytest.approx(
        sub_b_expected
    )


def test_no_cast_for_below_day(get_dataframe):
    """
    Test that results aren't cast to date for smaller time buckets.
    """
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
            ),
            time_bucket="hour",
        )
    )
    assert isinstance(df.datetime[0], datetime)


def test_error_when_subs_locations_not_point_geom():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    with pytest.raises(ValueError, match="does not have longitude/latitude columns"):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01",
                "2016-01-07",
                spatial_unit=make_spatial_unit("admin", level=3),
            )
        )


def test_error_on_spatial_unit_mismatch():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("admin", level=3))

    with pytest.raises(
        ValueError,
        match="reference_location must have the same spatial unit as subscriber_locations.",
    ):
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
    with pytest.raises(ValueError, match="'NOT_A_STATISTIC' is not a valid statistic"):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            statistic="NOT_A_STATISTIC",
        )


def test_invalid_time_bucket_raises_error():
    """
    Test that passing an invalid time bucket raises an error.
    """
    with pytest.raises(
        ValueError, match="'NOT_A_BUCKET' is not a valid value for time_bucket"
    ):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            time_bucket="NOT_A_BUCKET",
        )


def test_invalid_reference_raises_error():
    """
    Test that passing an invalid reference location raises an error.
    """
    with pytest.raises(
        ValueError,
        match="Argument 'reference_location' should be an instance of BaseLocation class or a tuple of two floats. Got: str",
    ):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location="NOT_A_LOCATION",
        )
