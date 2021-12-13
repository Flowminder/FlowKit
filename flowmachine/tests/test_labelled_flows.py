import pytest

from json import loads

from flowmachine.features.location.labelled_flows import LabelledFlows
from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberHandsetCharacteristic, Flows
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.features.subscriber.daily_location import locate_subscribers


@pytest.fixture
def labelled_flows():
    loc_1 = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )

    loc_2 = locate_subscribers(
        "2016-01-02",
        "2016-01-03",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )

    labels_1 = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    )

    return LabelledFlows(loc1=loc_1, loc2=loc_2, labels=labels_1)


def test_labelled_flow(labelled_flows, get_dataframe):

    foo = labelled_flows.get_query()
    df = get_dataframe(labelled_flows)
    assert len(df) == 349
    # We lose some subscribers between locations
    assert df.value.sum() == 490

    labelled_outflow = labelled_flows.outflow()
    bar = labelled_outflow.get_query()
    out_df = get_dataframe(labelled_outflow)
    assert all(out_df.columns == ["pcod_from", "value_label", "value"])


def test_spatial_unit_validation():
    loc_1 = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )

    loc_2 = locate_subscribers(
        "2016-01-02",
        "2016-01-03",
        spatial_unit=make_spatial_unit("admin", level=2),
        method="most-common",
    )

    labels_1 = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    )

    with pytest.raises(ValueError, match="same spatial unit"):
        _ = LabelledFlows(loc1=loc_1, loc2=loc_2, labels=labels_1)


def test_column_validation():
    loc_1 = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )

    loc_2 = locate_subscribers(
        "2016-01-02",
        "2016-01-03",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )

    labels_1 = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    )
    with pytest.raises(ValueError, match="not present in"):
        _ = LabelledFlows(
            loc1=loc_1, loc2=loc_2, labels=labels_1, label_columns=["notavalue"]
        )


def test_geojson(labelled_flows):

    out = labelled_flows.to_geojson_string()
    dict = loads(out)
    assert all(
        test in dict["features"][0]["properties"]["outflows"].keys()
        for test in ["Smart", "Feature"]
    )
    assert all(
        test in dict["features"][0]["properties"]["inflows"].keys()
        for test in ["Smart", "Feature"]
    )
