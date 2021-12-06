import pytest

from flowmachine.features.subscriber.mobility_classification import (
    MobilityClassification,
)
from flowmachine.features.subscriber.majority_location import MajorityLocation
from flowmachine.features.subscriber.location_visits import LocationVisits
from flowmachine.features.subscriber.day_trajectories import DayTrajectories
from flowmachine.features.subscriber.modal_location import ModalLocation
from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.core.spatial_unit import make_spatial_unit

import pandas as pd


@pytest.fixture
def prestored_majority_locations(flowmachine_connect):
    periods = [
        pd.date_range("2016-01-01", "2016-01-03"),
        pd.date_range("2016-01-02", "2016-01-04"),
        pd.date_range("2016-01-03", "2016-01-05"),
    ]
    modal_window_length = 3
    majority_locations = [
        MajorityLocation(
            subscriber_location_weights=LocationVisits(
                day_trajectories=DayTrajectories(
                    *(
                        ModalLocation(
                            *(
                                daily_location(
                                    daily_date.date().isoformat(),
                                    spatial_unit=make_spatial_unit("admin", level=2),
                                    method="last",
                                    table=["events.calls", "events.sms", "events.mds"],
                                )
                                for daily_date in pd.date_range(
                                    modal_start_date, periods=modal_window_length
                                )
                            )
                        )
                        for modal_start_date in period
                    )
                )
            ),
            weight_column="value",
            include_unlocatable=True,
        )
        for period in periods
    ]
    futures = [ml.store(store_dependencies=True) for ml in majority_locations]
    yield majority_locations


def test_mobility_classification(prestored_majority_locations, get_dataframe):
    # Note: pre-storing the location queries here gets the execution time down from 5 minutes to 7 seconds in my local tests
    mc = MobilityClassification(
        locations=prestored_majority_locations, stay_length_threshold=2
    )
    res = get_dataframe(mc)
    final_location_result = get_dataframe(prestored_majority_locations[-1])
    # Should return one label for each subscriber in the final location query
    assert len(final_location_result) == len(res)
    # Should return the expected columns
    assert list(res.columns) == mc.column_names
    # Should provide a label for every included subscriber
    assert not res["value"].isnull().values.any()
    # Check a few specific values
    assert res.set_index("subscriber").loc["87mobXjNNDVX51ML", "value"] == "mobile"
    assert (
        res.set_index("subscriber").loc["0W71ObElrz5VkdZw", "value"]
        == "not_always_locatable"
    )
    assert res.set_index("subscriber").loc["0DB8zw67E9mZAPK2", "value"] == "stable"
    assert res.set_index("subscriber").loc["DzpZJ2EaVQo2X5vM", "value"] == "unlocated"
