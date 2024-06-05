# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine.features import Displacement, daily_location
from numpy import isnan

from flowmachine.core import make_spatial_unit
from unittest.mock import Mock


@pytest.mark.parametrize(
    "stat, unit, sub_a_expected, sub_b_expected",
    [
        ("max", "km", 622.28125098963, 677.14603390011),
        ("median", "km", 398.64699488375, 442.22387282014),
        ("stddev", "km", 182.885304966881, 218.698764480622),
        ("avg", "km", 370.301009034846, 389.155343931983),
        ("sum", "km", 11109.0302710454, 11285.5049740275),
        ("variance", "km", 33447.0347728291, 47829.1495853505),
        ("max", "m", 622281.25098963, 677146.03390011),
        ("variance", "m", 33447034772.8291, 47829149585.3505),
    ],
)
def test_returns_expected_values(
    stat, unit, sub_a_expected, sub_b_expected, get_dataframe
):
    """
    Test that we get expected return values for the various statistics
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    df = get_dataframe(
        Displacement(
            "2016-01-01", "2016-01-07", reference_location=rl, statistic=stat, unit=unit
        )
    ).set_index("subscriber")
    assert df.loc[sub_a_id].value == pytest.approx(sub_a_expected)
    assert df.loc[sub_b_id].value == pytest.approx(sub_b_expected)


def test_min_displacement_zero(get_dataframe):
    """
    When time period for diplacement and home location are the same min displacement
    should be zero for all subscribers
    """
    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", reference_location=rl, statistic="min")
    )

    assert df.value.sum() == 0


def test_pass_reference_location(get_dataframe):
    """
    Test that we can pass a home location object to the class
    """

    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))

    df = get_dataframe(
        Displacement("2016-01-01", "2016-01-07", reference_location=rl, statistic="avg")
    )
    df = df.set_index("subscriber")

    val = df.loc["j6QYNbMJgAwlVORP"].value
    assert val == pytest.approx(370.301009034846)


def test_error_when_reference_location_not_lon_lat():
    """
    Test that error is raised if home location passed to class
    is not using lon-lat spatial unit
    """

    rl = daily_location("2016-01-01")

    with pytest.raises(ValueError):
        Displacement("2016-01-01", "2016-01-02", reference_location=rl, statistic="avg")


def test_error_when_reference_location_is_not_a_base_location():
    """
    Test that error is raised if reference_locations is not an object of type `BaseLocation`.
    """
    not_an_instance_of_base_location = Mock()

    with pytest.raises(ValueError):
        Displacement(
            "2016-01-01",
            "2016-01-02",
            reference_location=not_an_instance_of_base_location,
            statistic="avg",
        )


def test_invalid_statistic_raises_error():
    """
    Test that passing an invalid statistic raises an error.
    """
    dl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    with pytest.raises(ValueError):
        Displacement(
            "2016-01-01", "2016-01-07", reference_location=dl, statistic="BAD_STATISTIC"
        )


def test_get_all_users_in_reference_location(get_dataframe):
    """
    This tests that displacement values are returned for all subscribers
    in the home location object.
    """

    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")

    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    rl.store()
    d = Displacement(
        p2[0], p2[1], reference_location=rl, return_subscribers_not_seen=True
    )

    ml_subscribers = set(get_dataframe(rl).subscriber)
    d_subscribers = set(get_dataframe(d).subscriber)

    assert not (ml_subscribers - d_subscribers)


def test_subscriber_with_home_loc_but_no_calls_is_nan(get_dataframe):
    """
    Test that a subscriber who has no activity between start and stop
    but has a home location returns a nan value
    """

    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")
    subscriber = "OdM7np8LYEp1mkvP"

    rl = daily_location(
        "2016-01-01",
        spatial_unit=make_spatial_unit("lon-lat"),
        subscriber_subset=[subscriber],
    )
    rl.store()
    d = Displacement(
        p2[0],
        p2[1],
        reference_location=rl,
        return_subscribers_not_seen=True,
        subscriber_subset=[subscriber],
    )

    df = get_dataframe(d).set_index("subscriber")
    sub = df.loc[subscriber].value
    assert sub is None


def test_subscriber_with_home_loc_but_no_calls_is_ommitted(get_dataframe):
    """
    Test that a subscriber who has no activity between start and stop
    but has a home location is omitted by default.
    """

    p2 = ("2016-01-01 12:01:00", "2016-01-01 15:20:00")
    subscriber = "OdM7np8LYEp1mkvP"

    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    d = Displacement(*p2, reference_location=rl)

    df = get_dataframe(d).set_index("subscriber")

    assert subscriber not in df
