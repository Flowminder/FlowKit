# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine small helper functions
"""
import unittest.mock
from pathlib import Path

import pytest
import pglast

from flowmachine.core.errors import BadLevelError
from flowmachine.utils.utils import (
    parse_datestring,
    proj4string,
    get_columns_for_level,
    getsecret,
)

from flowmachine.utils import time_period_add
from flowmachine.features import daily_location, EventTableSubset

from flowmachine.core.utils import _makesafe, pretty_sql


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


def test_parse():
    """Test that several variations on a datestring give the same date"""
    assert (
        parse_datestring("2016-01-01").date()
        == parse_datestring("2016-01-01 10:00").date()
    )
    assert (
        parse_datestring("2016-01-01").date()
        == parse_datestring("2016-01-01 10:00:00").date()
    )


def test_graph():
    """Test that dependency graph util runs and has some correct entries."""
    g = daily_location("2016-01-01").dependency_graph()
    sd = EventTableSubset(
        "2016-01-01", "2016-01-02", columns=["msisdn", "datetime", "location_id"]
    )
    assert "x{}".format(sd.md5) in g.nodes()


def test_proj4(flowmachine_connect):
    """Test that correct proj4 strings are returned."""
    wsg84 = "+proj=longlat +datum=WGS84 +no_defs"
    haiti = "+proj=lcc +lat_1=35.46666666666667 +lat_2=34.03333333333333 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    assert proj4string(flowmachine_connect) == wsg84  # Default
    assert proj4string(flowmachine_connect, wsg84) == wsg84
    assert proj4string(flowmachine_connect, 2770) == haiti  # Known proj4


def test_proj4_errors(flowmachine_connect):
    """Test that appropriate errors are raised for bad inputs."""
    with pytest.raises(ValueError):
        proj4string(flowmachine_connect, ("foo",))
    with pytest.raises(ValueError):
        proj4string(flowmachine_connect, 1)


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
    """Test that sql is prettified as expected."""
    sql = "select foo, beta, frog from (select * from octagon where mooses in ('bees')) z limit 9"
    prettied = "SELECT foo,\n       beta,\n       frog\nFROM (SELECT *\n      FROM octagon\n      WHERE mooses IN ('bees')) AS z\nLIMIT 9"
    assert pretty_sql(sql) == prettied


def test_sql_validation():
    """Test that sql gets validated."""
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
    """Test that get_columns_for_level raises correct errors"""
    with pytest.raises(error):
        get_columns_for_level(level, column_name)


def test_column_list():
    """Test that supplying the column name as a list returns it as a new list."""
    passed_cols = ["frogs", "dogs"]
    returned_cols = get_columns_for_level("admin0", passed_cols)
    assert passed_cols == returned_cols
    assert id(passed_cols) != id(returned_cols)


def test_datestring_parse_error():
    """Test that correct error is raised when failing to parse a datestring."""
    with pytest.raises(ValueError):
        parse_datestring("DEFINITELY NOT A DATE")


def test_get_secrets(monkeypatch):
    """Test getting a secret from the special /run/secrets directory."""
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    open_mock = unittest.mock.mock_open(read_data=the_secret)
    monkeypatch.setattr("builtins.open", open_mock)
    secret = getsecret(the_secret_name, "Not the secret")
    assert the_secret == secret
    open_mock.assert_called_once_with(Path("/run/secrets") / the_secret_name, "r")


def test_get_secrets_default(monkeypatch):
    """Test getting a secret falls back to provided default with the file being there."""
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    secret = getsecret(the_secret_name, the_secret)
    assert the_secret == secret
