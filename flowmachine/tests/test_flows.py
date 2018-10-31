# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
from flowmachine.features import daily_location
from flowmachine.features.location.flows import *
import pytest, os
from flowmachine.features.subscriber.daily_location import locate_subscribers

pytestmark = pytest.mark.usefixtures("skip_datecheck")


@pytest.mark.parametrize("query", [FlowDiv, FlowMul, FlowPow, FlowSub, FlowSum])
def test_column_names_math(query, exemplar_level_param):
    """ Test that column_names property matches head(0) for FlowMath"""
    flow = Flows(
        daily_location("2016-01-01", **exemplar_level_param),
        daily_location("2016-01-01", **exemplar_level_param),
    )
    query_instance = query(flow, flow)
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


@pytest.mark.parametrize("query", [InFlow, OutFlow])
def test_column_names_inout(query, exemplar_level_param):
    """ Test that column_names property matches head(0) for InFlow & OutFlow"""
    flow = Flows(
        daily_location("2016-01-01", **exemplar_level_param),
        daily_location("2016-01-01", **exemplar_level_param),
    )
    query_instance = query(flow)
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


def test_column_names_flow(exemplar_level_param):
    """ Test that column_names property matches head(0) for Flows"""
    flow = Flows(
        daily_location("2016-01-01", **exemplar_level_param),
        daily_location("2016-01-01", **exemplar_level_param),
    )
    assert flow.head(0).columns.tolist() == flow.column_names


def test_column_names_edgelist(exemplar_level_param):
    """ Test that column_names property matches head(0) for EdgeList"""
    flow = EdgeList(daily_location("2016-01-01", **exemplar_level_param).aggregate())
    assert flow.head(0).columns.tolist() == flow.column_names


def test_calculates_flows(get_dataframe):
    """
    Flows() are correctly calculated
    """
    dl1 = locate_subscribers("2016-01-01", "2016-01-02", level="admin3", method="last")
    dl2 = locate_subscribers("2016-01-02", "2016-01-03", level="admin3", method="last")
    flow = Flows(dl1, dl2)
    df = get_dataframe(flow)
    assert (
        df[(df.name_from == "Arghakhanchi") & (df.name_to == "Dadeldhura")][
            "count"
        ].values[0]
        == 2
    )
    assert (
        df[(df.name_from == "Salyan") & (df.name_to == "Kavrepalanchok")][
            "count"
        ].values[0]
        == 2
    )
    assert (
        df[(df.name_from == "Sankhuwasabha") & (df.name_to == "Myagdi")][
            "count"
        ].values[0]
        == 4
    )


def test_flows_geojson_correct():
    """
    Test that flows outputs expected geojson.
    """
    dl1 = locate_subscribers("2016-01-01", "2016-01-02", level="admin3", method="last")
    dl2 = locate_subscribers("2016-01-02", "2016-01-03", level="admin3", method="last")
    flow = Flows(dl1, dl2)
    fl_json = flow.to_geojson()
    directory = os.path.dirname(os.path.os.path.realpath(__file__))
    reference_file = os.path.join(directory, "./data/", "flows_reference.json")
    with open(reference_file) as fin:
        assert fl_json == json.load(fin)
