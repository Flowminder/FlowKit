import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberHandsetCharacteristic
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.features.subscriber.daily_location import locate_subscribers


def test_labelled_spatial_aggregate(get_dataframe):
    """
    Tests disaggregation by a label query
    """
    locations = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )
    metric = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-02", characteristic="hnd_type"
    )
    labelled = LabelledSpatialAggregate(locations=locations, subscriber_labels=metric)
    df = get_dataframe(labelled)
    assert len(df) == 50
    assert len(df.pcod.unique()) == 25
    assert len(df.label_value.unique()) == 2


def test_validation():
    with pytest.raises(ValueError, match="not a column of"):
        locations = locate_subscribers(
            "2016-01-01",
            "2016-01-02",
            spatial_unit=make_spatial_unit("admin", level=3),
            method="most-common",
        )
        metric = SubscriberHandsetCharacteristic(
            "2016-01-01", "2016-01-02", characteristic="hnd_type"
        )
        labelled = LabelledSpatialAggregate(
            locations=locations, subscriber_labels=metric, label_columns=["foo", "bar"]
        )
