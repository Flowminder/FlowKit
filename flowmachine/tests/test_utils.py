# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine small helper functions
"""
import datetime
import pytest
import pglast
import re
import textwrap
import unittest.mock
import IPython
from io import StringIO

from flowmachine.core import CustomQuery
from flowmachine.core.subscriber_subsetter import make_subscriber_subsetter
from flowmachine.features import daily_location, EventTableSubset
from flowmachine.utils import *


@pytest.mark.parametrize("crs", (None, 4326, "+proj=longlat +datum=WGS84 +no_defs"))
def test_proj4string(crs, flowmachine_connect):
    """
    Test proj4string behaviour for known codes
    """
    assert (
        proj4string(flowmachine_connect, crs) == "+proj=longlat +datum=WGS84 +no_defs"
    )


@pytest.mark.parametrize("crs", (-1, (1, 1)))
def test_proj4string_valueerror(crs, flowmachine_connect):
    """
    Test proj4string valueerrors for bad values
    """
    with pytest.raises(ValueError):
        proj4string(flowmachine_connect, crs)


def test_time_period_add():
    """
    flowmachine.utils.time_period_add does what it says on the tin.
    """

    assert time_period_add("2016-01-01", 3) == "2016-01-04"
    assert time_period_add("2017-12-31", 1) == "2018-01-01"


def test_time_period_add_other_units():
    """
    flowmachine.utils.time_period_add can also add hours and minutes
    """

    assert time_period_add("2016-01-01", 3, unit="hours") == "2016-01-01 03:00:00"
    assert (
        time_period_add("2017-12-31 01:10:00", 50, unit="minutes")
        == "2017-12-31 02:00:00"
    )


@pytest.mark.parametrize(
    "datestring",
    [
        "2016-01-01 10:00",
        "2016-01-01 10:00:00",
        "2016-01-01",
        datetime.date(2016, 1, 1),
        datetime.datetime(2016, 1, 1, 10, 10),
    ],
)
def test_parse(datestring):
    """
    Test that several variations on a datestring give the same date
    """
    assert parse_datestring(datestring).date() == datetime.date(2016, 1, 1)


def test_convert_number_to_str():
    """
    Test that makesafe returns a string when passed a integer or float
    """
    assert _makesafe(123) == "123"
    assert _makesafe(float("Inf")) == "'Infinity'::float"


def test_convert_list_to_str():
    """
    Test that makesafe returns a string when passed a list
    """
    assert _makesafe(["foo", "bar"]) == "ARRAY['foo','bar']"


def test_sql_prettified():
    """
    Test that sql is prettified as expected.
    """
    sql = "select foo, beta, frog from (select * from octagon where mooses in ('bees')) z limit 9"
    prettied = "SELECT foo,\n       beta,\n       frog\nFROM (SELECT *\n      FROM octagon\n      WHERE mooses IN ('bees')) AS z\nLIMIT 9"
    assert pretty_sql(sql) == prettied


def test_sql_validation():
    """
    Test that sql gets validated.
    """
    sql = "elect foo from mooses"
    with pytest.raises(pglast.parser.ParseError):
        pretty_sql(sql)


@pytest.mark.parametrize(
    "level, column_name, error",
    [
        ("polygon", None, ValueError),
        ("polygon", 9, TypeError),
        ("badlevel", None, BadLevelError),
    ],
)
def test_columns_for_level_errors(level, column_name, error):
    """
    Test that get_columns_for_level raises correct errors
    """
    with pytest.raises(error):
        get_columns_for_level(level, column_name)


def test_column_list():
    """
    Test that supplying the column name as a list returns it as a new list.
    """
    passed_cols = ["frogs", "dogs"]
    returned_cols = get_columns_for_level("admin0", passed_cols)
    assert passed_cols == returned_cols
    assert id(passed_cols) != id(returned_cols)


def test_datestring_parse_error():
    """
    Test that correct error is raised when failing to parse a datestring.
    """
    with pytest.raises(ValueError):
        parse_datestring("DEFINITELY NOT A DATE")


def test_get_secrets(monkeypatch):
    """
    Test getting a secret from the special /run/secrets directory.
    """
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    open_mock = unittest.mock.mock_open(read_data=the_secret)
    monkeypatch.setattr("builtins.open", open_mock)
    secret = getsecret(the_secret_name, "Not the secret")
    assert the_secret == secret
    open_mock.assert_called_once_with(Path("/run/secrets") / the_secret_name, "r")


def test_get_secrets_default(monkeypatch):
    """
    Test getting a secret falls back to provided default with the file being there.
    """
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    secret = getsecret(the_secret_name, the_secret)
    assert the_secret == secret


def test_convert_dict_keys_to_strings():
    """
    Test that any dict keys that are numbers are converted to strings.
    """
    d = {0: {0: "foo", 1: "bar"}, 1: {"A": "baz", 2: "quux"}}
    d_out_expected = {"0": {"0": "foo", "1": "bar"}, "1": {"A": "baz", "2": "quux"}}
    d_out = convert_dict_keys_to_strings(d)
    assert d_out_expected == d_out


def test_to_nested_list():
    """
    Test that a dictionary with multiple levels is correctly converted to a nested list of key-value pairs.
    """
    d = {"a": {"b": 1, "c": [2, 3, {"e": 4}], "d": [5, 6]}}
    expected = [("a", [("b", 1), ("c", [2, 3, [("e", 4)]]), ("d", [5, 6])])]
    assert expected == to_nested_list(d)


def test_sort_recursively():
    """
    Test that `sort_recursively` recursively sorts all components of the input dictionary.
    """
    d = {
        "cc": {"foo": 23, "bar": 42},
        "aa": [("quux2", 100), ("quux1", 200)],
        "bb": "hello",
    }
    d_sorted_expected = {
        "aa": [("quux1", 200), ("quux2", 100)],
        "bb": "hello",
        "cc": {"bar": 42, "foo": 23},
    }

    assert d_sorted_expected == sort_recursively(d)


def test_print_dependency_tree():
    """
    Test that the expected dependency tree is printed for a daily location query (with an explicit subset).
    """
    subscriber_subsetter = make_subscriber_subsetter(
        CustomQuery(
            "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 10",
            ["duration", "subscriber"],
        )
    )
    q = daily_location(
        date="2016-01-02",
        level="admin2",
        method="most-common",
        subscriber_subset=subscriber_subsetter,
    )

    expected_output = textwrap.dedent(
        """\
        <Query of type: MostFrequentLocation, query_id: 'xxxxx'>
          - <Query of type: JoinToLocation, query_id: 'xxxxx'>
             - <Query of type: _SubscriberCells, query_id: 'xxxxx'>
                - <Query of type: EventsTablesUnion, query_id: 'xxxxx'>
                   - <Query of type: EventTableSubset, query_id: 'xxxxx'>
                      - <Query of type: CustomQuery, query_id: 'xxxxx'>
                      - <Table: 'events.sms', query_id: 'xxxxx'>
                         - <Table: 'events.sms', query_id: 'xxxxx'>
                   - <Query of type: EventTableSubset, query_id: 'xxxxx'>
                      - <Query of type: CustomQuery, query_id: 'xxxxx'>
                      - <Table: 'events.calls', query_id: 'xxxxx'>
                         - <Table: 'events.calls', query_id: 'xxxxx'>
             - <Query of type: CellToAdmin, query_id: 'xxxxx'>
                - <Query of type: CellToPolygon, query_id: 'xxxxx'>
        """
    )

    s = StringIO()
    print_dependency_tree(q, stream=s)
    output = s.getvalue()
    output_with_query_ids_replaced = re.sub(r"\b[0-9a-f]+\b", "xxxxx", output)

    assert expected_output == output_with_query_ids_replaced


def test_calculate_dependency_graph():
    """
    Test that calculate_dependency_graph() runs and the returned graph has some correct entries.
    """
    query = daily_location("2016-01-01")
    G = calculate_dependency_graph(query, analyse=True)
    sd = EventTableSubset(
        start="2016-01-01",
        stop="2016-01-02",
        columns=["msisdn", "datetime", "location_id"],
    )
    assert f"x{sd.md5}" in G.nodes()
    assert G.nodes[f"x{sd.md5}"]["query_object"].md5 == sd.md5


def test_plot_dependency_graph():
    """
    Test that plot_dependency_graph() runs and returns the expected IPython.display objects.
    """
    query = daily_location(date="2016-01-02", level="admin2", method="most-common")
    output_svg = plot_dependency_graph(query, format="svg")
    output_png = plot_dependency_graph(query, format="png", width=600, height=200)

    assert isinstance(output_svg, IPython.display.SVG)
    assert isinstance(output_png, IPython.display.Image)
    assert output_png.width == 600
    assert output_png.height == 200
