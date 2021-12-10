import pytest

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

    return LabelledFlows(loc_from=loc_1, loc_to=loc_2, labels=labels_1)


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


def test_geojson(labelled_flows):

    foo = labelled_flows.to_geojson_file("/home/john/test_json.json")
    bar = 1
