# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Various simple utilities.
"""

import datetime
import logging
from contextlib import contextmanager
from pathlib import Path
from threading import get_ident
from typing import List

import redis_lock
from redis_lock import AlreadyAcquired

from ..core.errors import BadLevelError

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


def get_columns_for_level(level, column_name=None) -> List[str]:
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
        return ["name"]

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


@contextmanager
def rlock(redis_client, lock_id, holder_id=None):
    """
    A reentrant lock(ish). This lock can be reacquired using the same
    holder_id as that which currently holds it.

    Parameters
    ----------
    redis_client : redis.StrictRedis
        Client for a redis
    lock_id : str
        Identifier of the lock to try and acquire
    holder_id : bytes
        Identifier of the holder, defaults to `get_ident()`

    Notes
    -----
    Not a true reentrant lock because being held n times by one holder
    requires only 1 release. Only the _first_ holder can release.

    """
    if holder_id is None:
        holder_id = f"{get_ident()}".encode()
    logger.debug(f"My lock holder id is {holder_id}")
    logger.debug(
        f"Getting lock. Currently held by {redis_lock.Lock(redis_client, lock_id, id=holder_id).get_owner_id()}"
    )
    try:
        with redis_lock.Lock(redis_client, lock_id, id=holder_id):
            yield
    except AlreadyAcquired:
        yield
