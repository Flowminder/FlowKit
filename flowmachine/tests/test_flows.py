# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import os

import geojson
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import daily_location
from flowmachine.features.location.flows import *
from flowmachine.features.subscriber.daily_location import locate_subscribers

pytestmark = pytest.mark.usefixtures("skip_datecheck")


@pytest.mark.parametrize("query", [InFlow, OutFlow])
def test_column_names_inout(query, exemplar_spatial_unit_param):
    """Test that column_names property matches head(0) for InFlow & OutFlow"""
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
    dl1 = daily_location("2016-01-01", spatial_unit=make_spatial_unit("admin", level=3))
    dl2 = daily_location("2016-01-01", spatial_unit=make_spatial_unit("admin", level=2))
    with pytest.raises(ValueError):
        Flows(dl1, dl2)


def test_column_names_flow(exemplar_spatial_unit_param):
    """Test that column_names property matches head(0) for Flows"""
    flow = Flows(
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
        daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
    )
    assert flow.head(0).columns.tolist() == flow.column_names


def test_calculates_flows(get_dataframe):
    """
    Flows() are correctly calculated
    """
    spatial_unit = make_spatial_unit("admin", level=3)
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
            "value"
        ].values[0]
        == 2
    )
    assert (
        df[(df.pcod_from == "524 4 10 53") & (df.pcod_to == "524 2 05 24")][
            "value"
        ].values[0]
        == 2
    )
    assert (
        df[(df.pcod_from == "524 1 02 09") & (df.pcod_to == "524 3 08 44")][
            "value"
        ].values[0]
        == 4
    )


def test_flows_geojson_correct():
    """
    Test that flows outputs expected geojson.
    """
    spatial_unit = make_spatial_unit("admin", level=3)
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


def test_valid_flows_geojson(exemplar_spatial_unit_param):
    """
    Check that valid geojson is returned for Flows.

    """
    if not exemplar_spatial_unit_param.has_geography:
        pytest.skip("Query with spatial_unit=CellSpatialUnit() has no geometry.")
    dl = daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param)
    dl2 = daily_location("2016-01-02", spatial_unit=exemplar_spatial_unit_param)
    fl = Flows(dl, dl2)
    assert geojson.loads(fl.to_geojson_string()).is_valid


def test_flows_geo_augmented_query_raises_error():
    """
    Test that a ValueError is raised when attempting to get geojson for a flows
    query with no geography data.
    """
    dl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("cell"))
    dl2 = daily_location("2016-01-02", spatial_unit=make_spatial_unit("cell"))
    fl = Flows(dl, dl2)
    with pytest.raises(ValueError):
        fl.to_geojson_string()


def test_flows_geojson(get_dataframe):
    """
    Test geojson works for flows with non-standard column names.
    """

    dl = daily_location(
        "2016-01-01",
        spatial_unit=make_spatial_unit(
            "admin", level=2, region_id_column_name="admin2name"
        ),
    )
    dl2 = daily_location(
        "2016-01-02",
        spatial_unit=make_spatial_unit(
            "admin", level=2, region_id_column_name="admin2name"
        ),
    )
    fl = Flows(dl, dl2)
    js = fl.to_geojson()
    df = get_dataframe(fl)
    check_features = [js["features"][0], js["features"][5], js["features"][7]]
    for feature in check_features:
        outflows = feature["properties"]["outflows"]
        df_src = df[
            df.admin2name_from == feature["properties"]["admin2name"]
        ].set_index("admin2name_to")
        for dest, tot in outflows.items():
            assert tot == df_src.loc[dest]["value"]


def test_flows_outer_join(get_dataframe):
    """
    Test that outer_join returns appropriate pieces
    """
    flow = Flows(
        daily_location("2016-01-01", spatial_unit=make_spatial_unit("admin", level=3)),
        daily_location("2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)),
        join_type="left outer",
    )
    out = get_dataframe(flow)
    assert out.pcod_to.isna().any()
    assert not out.pcod_from.isna().any()
