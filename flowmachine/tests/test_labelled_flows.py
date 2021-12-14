# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

import pytest

from json import loads

from flowmachine.features.location.redacted_labelled_spatial_query import (
    RedactedLabelledSpatialQuery,
)
from flowmachine.features.location.labelled_flows import LabelledFlows
from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberHandsetCharacteristic
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

    labels = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    )

    return LabelledFlows(loc1=loc_1, loc2=loc_2, labels=labels)


@pytest.fixture
def multi_labelled_flows():
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

    labels = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    ).join(
        SubscriberHandsetCharacteristic(
            "2016-01-01", "2016-01-03", characteristic="brand"
        ),
        "subscriber",
        left_append="_hnd_type",
        right_append="_brand",
    )
    return LabelledFlows(
        loc1=loc_1,
        loc2=loc_2,
        labels=labels,
        label_columns=["value_hnd_type", "value_brand"],
    )


def test_labelled_flow(labelled_flows, get_dataframe):

    df = get_dataframe(labelled_flows)
    assert len(df) == 349
    # We lose some subscribers between locations
    assert df.value.sum() == 490

    labelled_outflow = labelled_flows.outflow()
    out_df = get_dataframe(labelled_outflow)
    assert all(out_df.columns == ["pcod_from", "value_label", "value"])


def test_multiple_labels(get_dataframe, multi_labelled_flows):

    assert multi_labelled_flows.out_label_columns == [
        "value_hnd_type_label",
        "value_brand_label",
    ]
    assert multi_labelled_flows.column_names == [
        "pcod_from",
        "pcod_to",
        "value_hnd_type_label",
        "value_brand_label",
        "value",
    ]
    df = get_dataframe(multi_labelled_flows)
    assert len(df) > 300
    assert df.value.sum() == 490


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


def test_subscriber_validation():
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
    with pytest.raises(ValueError, match="subscriber"):
        _ = LabelledFlows(
            loc1=loc_1, loc2=loc_2, labels=labels_1, label_columns=["subscriber"]
        )


def test_label_redaction(get_dataframe):
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

    labels = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    ).join(
        SubscriberHandsetCharacteristic(
            "2016-01-01", "2016-01-03", characteristic="brand"
        ),
        "subscriber",
        left_append="_hnd_type",
        right_append="_brand",
    )
    lf = LabelledFlows(
        loc1=loc_1,
        loc2=loc_2,
        labels=labels,
        label_columns=["value_hnd_type"],
    )
    assert ["value_hnd_type_label"] == lf.out_label_columns
    assert all(
        ["pcod_from", "pcod_to", "value_hnd_type_label", "value"]
        == get_dataframe(lf).columns
    )


def test_geojson(labelled_flows):

    out = labelled_flows.to_geojson_string()
    dict = loads(out)
    assert all(
        test in dict["features"][0]["properties"]["outflows"].keys()
        for test in ["Smart", "Feature"]
    )
    assert (
        "524 1 01 04" in dict["features"][0]["properties"]["outflows"]["Feature"].keys()
    )
    assert all(
        test in dict["features"][0]["properties"]["inflows"].keys()
        for test in ["Smart", "Feature"]
    )
    assert (
        "524 3 08 43" in dict["features"][0]["properties"]["inflows"]["Feature"].keys()
    )
    assert dict["features"][0]["properties"]["inflows"]["Feature"]["524 3 08 43"] == 5


def test_geojson_multi_labels(multi_labelled_flows):
    out = multi_labelled_flows.to_geojson_string()
    dict = loads(out)
    assert all(
        brand in dict["features"][0]["properties"]["outflows"]["Feature"].keys()
        for brand in ["Nokia", "Apple", "Sony"]
    )
    assert (
        dict["features"][0]["properties"]["outflows"]["Feature"]["Nokia"]["524 3 08 43"]
        == 1
    )


def test_redacted_labelled_flows(labelled_flows, get_dataframe):
    redacted_flows = RedactedLabelledSpatialQuery(
        labelled_query=labelled_flows, redaction_threshold=3
    )
    df = get_dataframe(redacted_flows)
    assert len(df) < 10
    assert df.value.max() > 3


def test_redacted_multi_labelled_flows(multi_labelled_flows, get_dataframe):
    redacted_flows = RedactedLabelledSpatialQuery(
        labelled_query=multi_labelled_flows, redaction_threshold=1
    )
    df = get_dataframe(redacted_flows)
    assert len(df) == 19
    assert df.value.max() > 1
