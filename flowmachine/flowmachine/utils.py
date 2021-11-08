# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Various simple utilities.
"""

import datetime
import re
from functools import singledispatch

from pglast import prettify
from psycopg2._psycopg import adapt
from time import sleep
from typing import Union, Tuple


def parse_datestring(
    datestring: Union[str, datetime.datetime, datetime.date]
) -> datetime.datetime:
    """

    Parameters
    ----------
    datestring : str, datetime, or date
        ISO string date, or a datetime or date object

    Returns
    -------
    datetime.datetime

    """
    if isinstance(datestring, datetime.datetime):
        return datestring
    elif isinstance(datestring, datetime.date):
        return datetime.datetime(datestring.year, datestring.month, datestring.day)
    else:
        try:
            return datetime.datetime.strptime(datestring, "%Y-%m-%d %X")
        except ValueError:
            try:
                return datetime.datetime.strptime(datestring, "%Y-%m-%d %H:%M")
            except ValueError:
                try:
                    return datetime.datetime.strptime(datestring, "%Y-%m-%d")
                except ValueError:
                    try:
                        return datetime.datetime.fromisoformat(datestring)
                    except ValueError:
                        raise ValueError(
                            "{} could not be parsed as as date.".format(datestring)
                        )


def standardise_date(
    date: Union[str, datetime.date, datetime.datetime]
) -> Union[str, None]:
    """
    Turn a date, datetime, or date string into a standardised date string (YYYY-MM-DD HH:MM:SS).


    Parameters
    ----------
    date : str, date or datetime
        Date-like-thing to standardise.

    Returns
    -------
    str or None
       YYYY-MM-DD HH:MM:SS formatted date string or None if input was None.
    """
    if date is None:
        return date
    try:
        return date.strftime("%Y-%m-%d %H:%M:%S")
    except AttributeError:
        return parse_datestring(date).strftime("%Y-%m-%d %H:%M:%S")


def standardise_date_to_datetime(
    date: Union[str, datetime.date, datetime.datetime]
) -> Union[datetime.datetime, None]:
    """
    As `standardise_date` but returns a datetime object or None if input was None
    Parameters
    ----------
    date : str, date or datetime
        Date-like-thing to standardise.

    Returns
    -------
    datetime

    """
    if date is None:
        return None
    return datetime.datetime.strptime(standardise_date(date), "%Y-%m-%d %H:%M:%S")


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
    '2016-01-04 00:00:00'
    """
    new_date = parse_datestring(date) + datetime.timedelta(**{unit: n})

    return standardise_date(new_date)


def get_dist_query_string(*, lon1, lat1, lon2, lat2):
    """
    function for getting the distance
    query string between two lon-lat points.
    """
    return f"""
    ST_Distance(ST_Point({lon1}, {lat1})::geography,
                ST_point({lon2}, {lat2})::geography)
    """


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


def get_name_and_alias(column_name: str) -> Tuple[str]:
    """
    Given a column name string, return the column name and alias (if there is
    one), or return the provided column name twice if there is no alias.

    Examples
    --------
    >>> get_name_and_alias("col AS alias")
      ('col', 'alias')
    >>> get_name_and_alias("col")
      ('col', 'col')
    >>> get_name_and_alias("table.col")
      ('table.col', 'col')
    >>> get_name_and_alias("table.col as alias")
      ('table.col', 'alias')
    """
    column_name_split = re.split(" as ", column_name, flags=re.IGNORECASE)
    if len(column_name_split) == 1:
        return column_name_split[0].strip(), column_name_split[0].strip().split(".")[-1]
    else:
        return column_name_split[0].strip(), column_name_split[-1].strip()


def _sleep(seconds_to_sleep):
    # Private function to facilitate testing
    # monkeypatch this to avoid needing to monkeypatch time.sleep
    sleep(seconds_to_sleep)


def convert_dict_keys_to_strings(input_dict):
    """
    Return a copy of the input dictionary where all keys are converted to strings.
    This is necessary if the dictionary needs to be valid JSON.

    Parameters
    ----------
    input_dict : dict
        The input dictionary.

    Returns
    -------
    dict
        A copy of `input_dict` with all keys converted to strings.
    """
    result = {}
    for key, value in input_dict.items():
        new_key = str(key)
        if isinstance(value, dict):
            result[new_key] = convert_dict_keys_to_strings(value)
        else:
            result[new_key] = value
    return result


def to_nested_list(d):
    """
    Helper function to recursively convert an input dictionary to a nested list.

    Parameters
    ----------
    d : dict
        Input dictionary (can be nested).

    Returns
    -------
    list of tuples
    """
    if isinstance(d, dict):
        return [(key, to_nested_list(value)) for key, value in d.items()]
    elif isinstance(d, (list, tuple)):
        return [to_nested_list(x) for x in d]
    else:
        return d


def sort_recursively(d):
    """
    Helper function to sort a dictionary recursively (including any dicts or sequences inside it).

    Parameters
    ----------
    d : dict
        Input dictionary (can be nested).

    Returns
    -------
    dict
        The sorted dictionary.
    """
    if isinstance(d, dict):
        d_new = dict()
        for key in sorted(d.keys()):
            d_new[key] = sort_recursively(d[key])
        return d_new
    elif isinstance(d, list):
        return sorted(d, key=to_nested_list)
    else:
        return d


@singledispatch
def make_where(condition) -> str:
    """
    Create an sql where clause from a list of conditions.

    Parameters
    ----------
    condition : str or list of str
        Where conditions to combine

    Returns
    -------
    str
        A where clause or empty string if all clauses are empty

    Examples
    --------
    >>> make_where("")
    ""
    >>> make_where("NOT outgoing")
    "WHERE NOT outgoing"
    >>> make_where(["NOT outgoing", "duration > 5")
    "WHERE NOT outgoing AND duration > 5"

    """
    if condition.strip() != "":
        return f"WHERE {condition}"
    return ""


@make_where.register
def _(condition: list) -> str:
    non_empty = set(cond for cond in condition if cond.strip() != "")
    if len(non_empty) > 0:
        return f"WHERE {' AND '.join(non_empty)}"
    return ""
