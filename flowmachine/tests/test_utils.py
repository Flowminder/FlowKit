# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine small helper functions
"""
import pytest
import pglast

from flowmachine.core.context import get_db
from flowmachine.utils import *
from flowmachine.utils import _makesafe


@pytest.mark.parametrize(
    "date, expected",
    [
        (None, None),
        (datetime.date(2016, 1, 1), "2016-01-01 00:00:00"),
        (datetime.datetime(2016, 1, 1), "2016-01-01 00:00:00"),
        ("2016-01-01", "2016-01-01 00:00:00"),
        ("2016-01-01T00:00:00", "2016-01-01 00:00:00"),
        ("2016-01-01 00:00:00", "2016-01-01 00:00:00"),
    ],
)
def test_standardise_date(date, expected):
    assert standardise_date(date) == expected


@pytest.mark.parametrize(
    "date, expected",
    [
        (None, None),
        ("2016-01-01 00:00:00", datetime.datetime(2016, 1, 1)),
        ("2016-01-01", datetime.datetime(2016, 1, 1)),
        ("2016-01-01T00:00:00", datetime.datetime(2016, 1, 1)),
        ("2016-01-01 00:00:00", datetime.datetime(2016, 1, 1)),
    ],
)
def test_standardise_date_to_datetime(date, expected):
    assert standardise_date_to_datetime(date) == expected


@pytest.mark.parametrize("crs", (None, 4326, "+proj=longlat +datum=WGS84 +no_defs"))
def test_proj4string(crs, flowmachine_connect):
    """
    Test proj4string behaviour for known codes
    """
    assert proj4string(get_db(), crs) == "+proj=longlat +datum=WGS84 +no_defs"


@pytest.mark.parametrize("crs", (-1, (1, 1)))
def test_proj4string_valueerror(crs, flowmachine_connect):
    """
    Test proj4string valueerrors for bad values
    """
    with pytest.raises(ValueError):
        proj4string(get_db(), crs)


def test_time_period_add():
    """
    flowmachine.utils.time_period_add does what it says on the tin.
    """

    assert time_period_add("2016-01-01", 3) == "2016-01-04 00:00:00"
    assert time_period_add("2017-12-31", 1) == "2018-01-01 00:00:00"


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


def test_datestring_parse_error():
    """
    Test that correct error is raised when failing to parse a datestring.
    """
    with pytest.raises(ValueError):
        parse_datestring("DEFINITELY NOT A DATE")


@pytest.mark.parametrize(
    "column_name, name, alias",
    [
        ("column", "column", "column"),
        ("column AS alias", "column", "alias"),
        ("column as alias", "column", "alias"),
        ("table.column", "table.column", "column"),
        ("table.column AS alias", "table.column", "alias"),
    ],
)
def test_get_name_and_alias(column_name, name, alias):
    """
    Test that get_name_and_alias correctly splits a column name into name and alias.
    """
    assert (name, alias) == get_name_and_alias(column_name)


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
