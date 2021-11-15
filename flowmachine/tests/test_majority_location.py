from flowmachine.features import DayTrajectories, daily_location
from flowmachine.features.location.majority_location import MajorityLocation
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.subscriber.location_visits import LocationVisits
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


def test_majority_location(get_dataframe):

    # TODO Monday: Figure out why LocationVisits doesn't match it's docs.

    lv = LocationVisits(
        DayTrajectories(
            daily_location("2016-01-01"),
            daily_location("2016-01-02"),
            daily_location("2016-01-03"),
            daily_location("2016-01-04"),
        )
    )
    ml = MajorityLocation(lv, "dl_count")
    out = get_dataframe(ml)
    assert len(out) == 15
    assert out.subscriber.is_unique
    target = df.from_records(
        [
            ["1QBlwRo4Kd5v3Ogz", "524 3 08 44"],
            ["37J9rKydzJ0mvo0z", "524 4 12 62"],
            ["3XKdxqvyNxO2vLD1", "524 4 12 62"],
            ["81B6q0K8k325XWmn", "524 1 03 13"],
            ["8dXPM6JXj05qwjW0", "524 4 12 62"],
            ["bKZLwjrMQG7z468y", "524 4 12 62"],
            ["g9D5KEK9BQzOa8z0", "524 4 11 57"],
        ],
        columns=["subscriber", "pcod"],
    )
    assert_frame_equal(out.head(7), target)
