# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import os

import pytest

from flowmachine.core.spatial_unit import AdminSpatialUnit
from flowmachine.features import daily_location
from flowmachine.features.location.flows import *
from flowmachine.features.subscriber.daily_location import locate_subscribers

pytestmark = pytest.mark.usefixtures("skip_datecheck")


@pytest.mark.parametrize("query", [InFlow, OutFlow])
def test_column_names_inout(query, exemplar_spatial_unit_param):
    """ Test that column_names property matches head(0) for InFlow & OutFlow"""
    flow = Flows(
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
    )
    query_instance = query(flow)
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


def test_flows_raise_error():
    """
    Flows() raises error if location levels are different.
    """
    dl1 = daily_location("2016-01-01", spatial_unit=AdminSpatialUnit(level=3))
    dl2 = daily_location("2016-01-01", spatial_unit=AdminSpatialUnit(level=3))
    with pytest.raises(ValueError):
        Flows(dl1, dl2)


def test_column_names_flow(exemplar_spatial_unit_param):
    """ Test that column_names property matches head(0) for Flows"""
    flow = Flows(
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
    )
    assert flow.head(0).columns.tolist() == flow.column_names


def test_calculates_flows(get_dataframe):
    """
    Flows() are correctly calculated
    """
    spatial_unit = AdminSpatialUnit(level=3)
    dl1 = locate_subscribers(
        "2016-01-01", "2016-01-02", spatial_unit=spatial_unit, method="last"
    )
    dl2 = locate_subscribers(
        "2016-01-02", "2016-01-03", spatial_unit=spatial_unit, method="last"
    )
    flow = Flows(dl1, dl2)
    df = get_dataframe(flow)
    assert (
        df[(df.pcod_from == "524 3 09 50") & (df.pcod_to == "524 5 14 73")][
            "count"
        ].values[0]
        == 2
    )
    assert (
        df[(df.pcod_from == "524 4 10 53") & (df.pcod_to == "524 2 05 24")][
            "count"
        ].values[0]
        == 2
    )
    assert (
        df[(df.pcod_from == "524 1 02 09") & (df.pcod_to == "524 3 08 44")][
            "count"
        ].values[0]
        == 4
    )


def test_flows_geojson_correct():
    """
    Test that flows outputs expected geojson.
    """
    spatial_unit = AdminSpatialUnit(level=3)
    dl1 = locate_subscribers(
        "2016-01-01", "2016-01-02", spatial_unit=spatial_unit, method="last"
    )
    dl2 = locate_subscribers(
        "2016-01-02", "2016-01-03", spatial_unit=spatial_unit, method="last"
    )
    flow = Flows(dl1, dl2)
    fl_json = flow.to_geojson()
    directory = os.path.dirname(os.path.os.path.realpath(__file__))
    reference_file = os.path.join(directory, "./data/", "flows_reference.json")
    with open(reference_file) as ref:
        ref_json = json.load(ref)
    assert ref_json == fl_json
