from flowmachine.features.subscriber.home_location_monthly import HomeLocationMonthly
from flowmachine.core.spatial_unit import AnySpatialUnit, AdminSpatialUnit
from flowmachine.core.query import Query


def test_home_location_monthly():

    home_location_monthly = HomeLocationMonthly(
        window_start="2016-01-07",
        window_stop="2016-01-10",
        agg_unit=AdminSpatialUnit(level=3),
        known_threshold=2,
        unknown_threshold=3,
    )

    home_location_next_month = HomeLocationMonthly(
        window_start="2016-01-11",
        window_stop="2016-01-14",
        agg_unit=AdminSpatialUnit(level=3),
        known_threshold=2,
        unknown_threshold=3,
        ref_location=home_location_monthly,
    )

    assert home_location_next_month.ref_location.window_start == "2016-01-07"
    out = next(iter(home_location_next_month))
    assert out
