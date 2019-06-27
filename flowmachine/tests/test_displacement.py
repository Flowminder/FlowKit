# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine.features import Displacement, ModalLocation, daily_location
from numpy import isnan

from flowmachine.utils import list_of_dates
from flowmachine.core import make_spatial_unit


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("max", 500.809349, 387.024628),
        ("median", 130.327306, 151.547503),
        ("stddev", 132.73844, 131.026341),
        ("avg", 176.903620, 178.910994),
        ("sum", 5307.10860, 5188.418836),
        ("variance", 17619494.3698313, 17167902.1823453),
    ],
)
def test_returns_expected_values(stat, sub_a_expected, sub_b_expected, get_dataframe):
    """
    Test that we get expected return values for the various statistics
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", statistic=stat)
    ).set_index("subscriber")
    assert df.loc[sub_a_id].statistic == pytest.approx(sub_a_expected)
    assert df.loc[sub_b_id].statistic == pytest.approx(sub_b_expected)


def test_returns_expected_result_for_unit_m(get_dataframe):
    """
    Test that we get expected results when unit='m'.
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", statistic="max", unit="m")
    ).set_index("subscriber")
    assert df.loc[sub_a_id].statistic == pytest.approx(500809.349)
    assert df.loc[sub_b_id].statistic == pytest.approx(387024.628)


def test_min_displacement_zero(get_dataframe):
    """
    When time period for diplacement and home location are the same min displacement
    should be zero for all subscribers
    """

    df = get_dataframe(Displacement("2016-01-01", "2016-01-07", statistic="min"))

    assert df.statistic.sum() == 0


def test_pass_modal_location(get_dataframe):
    """
    Test that we can pass a home location object to the class
    """

    ml = ModalLocation(
        *[
            daily_location(d, spatial_unit=make_spatial_unit("lon-lat"))
            for d in list_of_dates("2016-01-01", "2016-01-06")
        ]
    )

    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", modal_locations=ml, statistic="avg")
    )
    df = df.set_index("subscriber")

    val = df.loc["j6QYNbMJgAwlVORP"].statistic
    assert val == pytest.approx(176.903620)


def test_error_when_modal_location_not_lon_lat():
    """
    Test that error is raised if home location passed to class
    is not using lon-lat spatial unit
    """

    ml = ModalLocation(
        *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-02")]
    )

    with pytest.raises(ValueError):
        Displacement("2016-01-01", "2016-01-02", modal_locations=ml, statistic="avg")


def test_error_when_not_modal_location():
    """
    Test that error is raised if modal_locations is not a ModalLocation.
    """
    dl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    with pytest.raises(ValueError):
        Displacement("2016-01-01", "2016-01-02", modal_locations=dl, statistic="avg")


def test_invalid_statistic_raises_error():
    """
    Test that passing an invalid statistic raises an error.
    """
    with pytest.raises(ValueError):
        Displacement("2016-01-01", "2016-01-07", statistic="BAD_STATISTIC")


def test_get_all_users_in_modal_location(get_dataframe):
    """
    This tests that diplacement values are returned for all subscribers
    in the home location object.
    """

    p1 = ("2016-01-02 10:00:00", "2016-01-02 12:00:00")
    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")

    ml = ModalLocation(
        *[
            daily_location(d, spatial_unit=make_spatial_unit("lon-lat"), hours=(12, 13))
            for d in list_of_dates(p1[0], p1[1])
        ]
    )
    d = Displacement(p2[0], p2[1], modal_locations=ml)

    ml_subscribers = set(get_dataframe(ml).subscriber)
    d_subscribers = set(get_dataframe(d).subscriber)

    assert not (ml_subscribers - d_subscribers)


def test_subscriber_with_home_loc_but_no_calls_is_nan(get_dataframe):
    """
    Test that a subscriber who has no activity between start and stop
    but has a home location returns a nan value
    """

    p1 = ("2016-01-02 10:00:00", "2016-01-02 12:00:00")
    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")
    subscriber = "OdM7np8LYEp1mkvP"

    ml = ModalLocation(
        *[
            daily_location(d, spatial_unit=make_spatial_unit("lon-lat"), hours=(12, 13))
            for d in list_of_dates(p1[0], p1[1])
        ]
    )
    d = Displacement(p2[0], p2[1], modal_locations=ml)

    df = get_dataframe(d).set_index("subscriber")

    assert isnan(df.loc[subscriber].statistic)
