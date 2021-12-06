import pytest

from flowmachine.features.subscriber.mobility_classification import (
    MobilityClassification,
)
from flowmachine.features.subscriber.majority_location import MajorityLocation
from flowmachine.features.subscriber.subscriber_call_durations import (
    PerLocationSubscriberCallDurations,
)
from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.core.spatial_unit import make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError


@pytest.mark.parametrize(
    "spatial_unit_params",
    [
        {"spatial_unit_type": "admin", "level": 3},
        {"spatial_unit_type": "versioned-cell"},
    ],
)
def test_mobility_classification_result(get_dataframe, spatial_unit_params):
    """
    Test that the result of a MobilityClassification query is as expected
    """
    spatial_unit = make_spatial_unit(**spatial_unit_params)
    locations = [
        MajorityLocation(
            subscriber_location_weights=PerLocationSubscriberCallDurations(
                "2016-01-01", "2016-01-02", statistic="count", spatial_unit=spatial_unit
            ),
            weight_column="value",
            include_unlocatable=True,
        ),
        MajorityLocation(
            subscriber_location_weights=PerLocationSubscriberCallDurations(
                "2016-01-02", "2016-01-03", statistic="count", spatial_unit=spatial_unit
            ),
            weight_column="value",
            include_unlocatable=True,
        ),
    ]
    mc = MobilityClassification(
        locations=locations,
        stay_length_threshold=2,
    )
    res = get_dataframe(mc)
    final_location_result = get_dataframe(locations[-1])
    # Should return one label for each subscriber in the final location query
    assert len(final_location_result) == len(res)
    assert res.subscriber.is_unique
    # Should return the expected columns
    assert list(res.columns) == mc.column_names
    # Should provide a label for every included subscriber
    assert not res["value"].isnull().values.any()
    # Check a few specific values
    assert res.set_index("subscriber").loc["09NrjaNNvDanD8pk", "value"] == "irregular"
    assert res.set_index("subscriber").loc["1p4MYbA1Y4bZzBQa", "value"] == "mobile"
    assert (
        res.set_index("subscriber").loc["0MQ4RYeKn7lryxGa", "value"]
        == "not_always_locatable"
    )
    assert res.set_index("subscriber").loc["dgz4WNZOAm5bmJ9M", "value"] == "stable"
    assert res.set_index("subscriber").loc["0zqZPERbraE8NM4W", "value"] == "unlocated"


def test_mobility_classification_inconsistent_spatial_units_raises():
    """
    Test that MobilityClassification raises an error if the input locations
    do not all have the same spatial unit
    """
    with pytest.raises(InvalidSpatialUnitError, match="same spatial unit"):
        mc = MobilityClassification(
            locations=[
                daily_location(
                    "2016-01-01",
                    spatial_unit=make_spatial_unit("admin", level=2),
                ),
                daily_location(
                    "2016-01-01",
                    spatial_unit=make_spatial_unit("admin", level=3),
                ),
            ]
        )


def test_mobility_classification_stay_length_threshold(get_dataframe):
    """
    Test that the 'stay_length_threshold' parameter has the expected effect
    on stable/mobile classification
    """
    mc_small_threshold = MobilityClassification(
        locations=[
            daily_location(
                "2016-01-01",
                spatial_unit=make_spatial_unit("admin", level=2),
            ),
            daily_location(
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            ),
        ],
        stay_length_threshold=1,
    )
    mc_large_threshold = MobilityClassification(
        locations=[
            daily_location(
                "2016-01-01",
                spatial_unit=make_spatial_unit("admin", level=2),
            ),
            daily_location(
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            ),
        ],
        stay_length_threshold=3,
    )
    # stay_length_threshold=1 should mean no subscribers are 'mobile'
    assert "mobile" not in get_dataframe(mc_small_threshold)["value"].values
    # stay_length_threshold=3 (with 2 locations) should mean no subscribers are 'stable'
    assert "stable" not in get_dataframe(mc_large_threshold)["value"].values
