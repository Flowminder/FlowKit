from flowmachine.features.location.majority_location import MajorityLocation
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


def test_majority_location(get_dataframe):
    subscriber_subset = SubscriberLocations("2016-01-01", "2016-01-02")
    ml = MajorityLocation(subscriber_subset)
    sql = ml.get_query()
    out = get_dataframe(ml)
    assert len(out) == 28
    assert out.subscriber.is_unique
    target = df.from_records(
        [
            ["6kj8RD7YQ4kwWlQy", "AUQZGMW3"],
            ["7qKmzkeMbmk5nOa0", "w4H81eLM"],
            ["7XebRKr35JMJnq8A", "9eosMq9j"],
            ["8rxEQOL3ePZdAe1z", "xaFQVHqu"],
            ["aK9GDX8nozb2RB67", "ns6vzdkC"],
        ],
        columns=["subscriber", "location_id"],
    )
    assert_frame_equal(out.head(5), target)
