# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine.features import Displacement, ModalLocation, daily_location
from numpy import isnan

from flowmachine.utils import list_of_dates


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("max", 521.925932, 426.989228),
        ("median", 112.337149, 144.098363),
        ("stddev", 160.84803, 152.864749),
        ("avg", 169.926194, 182.776455),
        ("sum", 5097.785810, 5300.517199),
        ("variance", 25872091.3161635, 23367631.4344498),
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
    assert df.loc[sub_a_id].statistic, pytest.approx(sub_a_expected)
    assert df.loc[sub_b_id].statistic, pytest.approx(sub_b_expected)


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

    hl = ModalLocation(
        *[
            daily_location(d, level="lat-lon")
            for d in list_of_dates("2016-01-01", "2016-01-06")
        ]
    )

    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", modal_locations=hl, statistic="avg")
    )
    df = df.set_index("subscriber")

    val = df.loc["j6QYNbMJgAwlVORP"].statistic
    assert val == pytest.approx(169.926194)


def test_error_when_modal_location_not_latlong():
    """
    Test that error is raised if home location passed to class
    is not using level lat-lon
    """

    hl = ModalLocation(
        *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-02")]
    )

    with pytest.raises(ValueError):
        Displacement("2016-01-01", "2016-01-02", modal_locations=hl, statistic="avg")


def test_get_all_users_in_modal_location(get_dataframe):
    """
    This tests that diplacement values are returned for all subscribers
    in the home location object.
    """

    p1 = ("2016-01-02 10:00:00", "2016-01-02 12:00:00")
    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")

    hl = ModalLocation(
        *[
            daily_location(d, level="lat-lon", hours=(12, 13))
            for d in list_of_dates(p1[0], p1[1])
        ]
    )
    d = Displacement(p2[0], p2[1], modal_locations=hl)

    hl_subscribers = set(get_dataframe(hl).subscriber)
    d_subscribers = set(get_dataframe(d).subscriber)

    assert not (hl_subscribers - d_subscribers)


def test_subscriber_with_home_loc_but_no_calls_is_nan(get_dataframe):
    """
    Test that a subscriber who has no activity between start and stop
    but has a home location returns a nan value
    """

    p1 = ("2016-01-02 10:00:00", "2016-01-02 12:00:00")
    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")
    subscriber = "OdM7np8LYEp1mkvP"

    hl = ModalLocation(
        *[
            daily_location(d, level="lat-lon", hours=(12, 13))
            for d in list_of_dates(p1[0], p1[1])
        ]
    )
    d = Displacement(p2[0], p2[1], modal_locations=hl)

    df = get_dataframe(d).set_index("subscriber")

    assert isnan(df.loc[subscriber].statistic)
