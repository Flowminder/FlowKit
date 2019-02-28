# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Various simple utilities.
"""
from time import sleep

import datetime
import logging

from pathlib import Path
from pglast import prettify
from psycopg2._psycopg import adapt


from typing import List, Union

import flowmachine
from flowmachine.core.errors import BadLevelError
from flowmachine.core.errors.flowmachine_errors import MissingColumnsError

logger = logging.getLogger("flowmachine").getChild(__name__)


def getsecret(key: str, default: str) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets), return a default if the file is not there.

    Parameters
    ----------
    key: str
        Name of the secret.
    default: str
        Default value to return if the file does not exist

    Returns
    -------
    str
        Value in the file, or default
    """
    try:
        with open(Path("/run/secrets") / key, "r") as fin:
            return fin.read().strip()
    except FileNotFoundError:
        return default


def get_columns_for_level(
    level: str, column_name: Union[str, List[str]] = None
) -> List[str]:
    """
    Get a list of the location related columns

    Parameters
    ----------
    level : {'cell', 'versioned-cell', 'versioned-site', 'lat-lon', 'grid', 'adminX'}
        Level to get location columns for
    column_name : str, or list of strings, optional
        name of the column or list of column names. None by default
        if this is not none then the function trivially returns the
        column name as a list.
    Returns
    -------
    relevant_columns : list
        A list of the database columns for this level

    Examples
    --------
    >>> get_columns_for_level("admin3") 
    ['name']

    """
    if level == "polygon" and not column_name:
        raise ValueError("Must pass a column name for level=polygon")

    if column_name:
        if isinstance(column_name, str):
            relevant_columns = [column_name]
        elif isinstance(column_name, list):
            relevant_columns = list(column_name)
        else:
            raise TypeError("column name should be a list or a string")
        return relevant_columns

    if level.startswith("admin"):
        return ["pcod"]

    returns = {
        "cell": ["location_id"],
        "versioned-cell": ["location_id", "version", "lon", "lat"],
        "versioned-site": ["site_id", "version", "lon", "lat"],
        "lat-lon": ["lat", "lon"],
        "grid": ["grid_id"],
    }

    try:
        return returns[level]
    except KeyError:
        raise BadLevelError(level)


def parse_datestring(datestring):
    """

    Parameters
    ----------
    datestring : str
        ISO string date

    Returns
    -------
    datetime.datetime

    """
    try:
        return datetime.datetime.strptime(datestring, "%Y-%m-%d %X")
    except ValueError:
        try:
            return datetime.datetime.strptime(datestring, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                return datetime.datetime.strptime(datestring, "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    "{} could not be parsed as as date.".format(datestring)
                )


def list_of_dates(start, stop):
    """

    Parameters
    ----------
    start, stop : str
        yyyy-mm-dd format datestrings.

    Returns
    -------
    list of yyyy-mm-dd format datestrings between start and stop (inclusive)

    """

    d1 = parse_datestring(start)
    d2 = parse_datestring(stop)

    if d2 < d1:
        raise ValueError("The start date is later than the stop date.")

    total_days = (d2 - d1).days

    # Note the plus one, as we want to include the final day
    all_dates = [
        (d1 + datetime.timedelta(i)).strftime("%Y-%m-%d") for i in range(total_days + 1)
    ]

    return all_dates


def time_period_add(date, n, unit="days"):
    """
    Adds n days to the date (represented as a string). Or alternatively add hours or
    minutes to the date.

    Parameters
    ----------
    date : str
        Date to add to in yyyy-mm-dd format
    n : int
        Number of units to add
    unit : str, default 'days'
        Type of unit to add to the date

    Returns
    -------
    str
        Altered date string

    Examples
    --------

    >>> time_period_add('2016-01-01', 3)
    '2016-01-04'
    """
    new_date = parse_datestring(date) + datetime.timedelta(**{unit: n})
    # Now for convenience if the date has no time component then we want to
    # simply return the date string, i.e. without the time component. Otherwise
    # we need to return the whole thing.
    date_string = new_date.strftime("%Y-%m-%d %X")
    if date_string.endswith("00:00:00"):
        return date_string.split(" ")[0]
    else:
        return date_string


def get_dist_string(lo1, la1, lo2, la2):
    """
    function for getting the distance
    query string between to lat-lon points.
    """
    return """
    ST_Distance(ST_Point({}, {})::geography,
                ST_point({}, {})::geography)
    """.format(
        lo1, la1, lo2, la2
    )


def proj4string(conn, crs=None):
    """
    Provide a proj4 string for the input, or by default
    return the wsg84 proj4 string.

    Parameters
    ----------
    conn : Connection
        FlowMachine db connection to use to get the proj4 string
    crs : int or str
        An integer EPSG code, or a proj4 string

    Returns
    -------
    str
        Proj4 string for the input crs.

    """
    if isinstance(crs, int):
        try:
            proj4_string = conn.fetch(
                "SELECT proj4text FROM spatial_ref_sys WHERE srid={}".format(crs)
            )[0][0]
        except IndexError:
            raise ValueError("{} is not a valid EPSG code.".format(crs))
    elif isinstance(crs, str):
        proj4_string = crs
    elif crs is None:
        proj4_string = "+proj=longlat +datum=WGS84 +no_defs"
    else:
        raise ValueError("{} cannot be converted to proj4.".format(crs))
    return proj4_string.strip()


def verify_columns_exist_in_all_tables(conn, tables, columns):
    """
    Parse a list of tables ensuring that certain columns are present.

    Parameters
    ----------
    conn : Connection
        FlowMachine db connection to use to get the subscriber_tables.
    tables : str or list of strings, default 'all'
        Can be a sting of a single table (with the schema) or a list of these.
        The keyword all is to select all subscriber tables
    columns : str or list
        A string or list of strings with the column names that must be present.

    Returns
    -------
    None
        The functions returns None when all columns exist in all tables.

    Raises
    ------
    MissingColumnsError
        If any column does not exist in any of the tables.
    """

    if isinstance(tables, str) and tables.lower() == "all":
        tables = [f"events.{t}" for t in conn.subscriber_tables]
    elif type(tables) is str:
        tables = [tables]
    else:
        tables = tables

    if isinstance(columns, str):
        columns = [columns]

    tables_lacking_columns = []
    for t in tables:
        for c in columns:
            if c not in flowmachine.core.Table(t).column_names:
                tables_lacking_columns.append(t)
                break

    if tables_lacking_columns:
        raise MissingColumnsError(tables_lacking_columns, columns)


def pretty_sql(
    sql,
    compact_lists_margin=0,
    split_string_literals_threshold=0,
    special_functions=True,
    comma_at_eoln=True,
):
    """
    Prettify and validate the syntax of an SQL query, using pglast

    Parameters
    ----------
    sql : str
        SQL to prettyify and validate
    compact_lists_margin : int, default 0
        Use compact form for lists shorter than this
    split_string_literals_threshold : int, default 0
        Split strings (in the sql) longer than this threshold
    special_functions : bool, default True
        Translate some special functions to their more commonly used forms
    comma_at_eoln

    Raises
    ------

    pglast.parser.ParseError
        Raises a parse error if the query syntax was bad.

    See Also
    --------
    pglast.prettify: Function wrapped, use this for additional prettification options

    Returns
    -------
    str
        The prettified string.

    """

    return prettify(
        sql,
        compact_lists_margin=compact_lists_margin,
        split_string_literals_threshold=split_string_literals_threshold,
        special_functions=special_functions,
        comma_at_eoln=comma_at_eoln,
    )


def _makesafe(x):
    """
    Function that converts input into a PostgreSQL readable.
    """
    return adapt(x).getquoted().decode()


def _sleep(seconds_to_sleep):
    # Private function to facilitate testing
    # monkeypatch this to avoid needing to monkeypatch time.sleep
    sleep(seconds_to_sleep)
