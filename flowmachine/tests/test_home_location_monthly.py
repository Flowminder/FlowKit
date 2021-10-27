from flowmachine.features.subscriber.home_location_monthly import HomeLocationMonthly
from flowmachine.core.spatial_unit import AnySpatialUnit, AdminSpatialUnit


def test_home_location_monthly():
    home_location_monthly = HomeLocationMonthly(
        window_start="2016-01-07",
        window_stop="2016-01-14",
        agg_unit=AdminSpatialUnit(level=3),
        known_threshold=4,
        unknown_threshold=2,
        cache=False,
    )
    sql = home_location_monthly.get_query()

    home_location_next_month = HomeLocationMonthly(
        window_start="2016-01-15",
        window_stop="2016-01-21",
        agg_unit=AdminSpatialUnit(level=3),
        known_threshold=4,
        unknown_threshold=2,
        cache=False,
        ref_location=home_location_monthly,
    )

    sql = home_location_next_month.get_query()
    foo = 1
