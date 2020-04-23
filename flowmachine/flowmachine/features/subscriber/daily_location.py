# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates an subscriber daily location using different
methods. A daily location is a statistic 
representing where an subscriber is on a given day.



"""
import datetime
from typing import Optional

from ...core import make_spatial_unit
from ...core.spatial_unit import AnySpatialUnit
from .last_location import LastLocation
from .most_frequent_location import MostFrequentLocation
from flowmachine.utils import parse_datestring, standardise_date


def locate_subscribers(
    start,
    stop,
    spatial_unit: Optional[AnySpatialUnit] = None,
    hours="all",
    method="last",
    table="all",
    subscriber_identifier="msisdn",
    *,
    ignore_nulls=True,
    subscriber_subset=None,
):
    """
    Return a class representing the location of an individual. This can be called
    with a number of different methods.

    Find the last/most-frequent location for every subscriber within the given time
    frame. Specify a spatial unit.

    Parameters
    ----------
    start, stop : str
        iso format date range for the the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of ints, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    method : str, default 'last'
        The method by which to calculate the location of the subscriber.
        This can be either 'most-common' or last. 'most-common' is
        simply the modal location of the subscribers, whereas 'lsat' is
        the location of the subscriber at the time of the final call in
        the data.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'all' it will use all tables that contain
        location data, specified in flowmachine.yml.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    kwargs :
        Eventually passed to flowmachine.spatial_metrics.spatial_helpers.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    Examples
    --------

    >>> last_locs = locate_subscribers('2016-01-01 13:30:30',
                                '2016-01-02 16:25:00'
                                 spatial_unit = CellSpatialUnit
                                 method='last')
    >>> last_locs.head()
                subscriber    |    cell
                subscriberA   |   233241
                subscriberB   |   234111
                subscriberC   |   234111
                        .
                        .
                        .
    """
    if spatial_unit is None:
        spatial_unit = make_spatial_unit("admin", level=3)

    if method == "last":
        return LastLocation(
            start,
            stop,
            spatial_unit,
            hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )
    elif method == "most-common":
        return MostFrequentLocation(
            start,
            stop,
            spatial_unit,
            hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )
    # elif self.method == 'first':
    #     _obj = FirstLocation(start, stop, spatial_unit, hours)
    else:
        raise ValueError(
            f"Unrecognised method '{method}', must be either 'most-common' or 'last'"
        )


def daily_location(
    date,
    stop=None,
    *,
    spatial_unit: Optional[AnySpatialUnit] = None,
    hours="all",
    method="last",
    table="all",
    subscriber_identifier="msisdn",
    ignore_nulls=True,
    subscriber_subset=None,
):
    """
    Return a query for locating all subscribers on a single day of data.

    Parameters
    ----------
    date : str
        iso format date for the day in question,
        e.g. 2016-01-01
    stop : str
        optionally specify a stop datetime in iso format date for the day in question,
        e.g. 2016-01-02 06:00:00
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of ints, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    method : str, default 'last'
        The method by which to calculate the location of the subscriber.
        This can be either 'most-common' or last. 'most-common' is
        simply the modal location of the subscribers, whereas 'lsat' is
        the location of the subscriber at the time of the final call in
        the data.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'ALL' it will use all tables that contain
        location data, specified in flowmachine.yml.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    """
    if spatial_unit is None:
        spatial_unit = make_spatial_unit("admin", level=3)

    # Temporary band-aid; marshmallow deserialises date strings
    # to date objects, so we convert it back here because the
    # lower-level classes still assume we are passing date strings.
    date = standardise_date(date)

    if stop is None:
        # 'cast' the date object as a date
        d1 = parse_datestring(date)
        # One day after this
        d2 = d1 + datetime.timedelta(1)
        stop = standardise_date(d2)
    return locate_subscribers(
        start=date,
        stop=stop,
        spatial_unit=spatial_unit,
        hours=hours,
        method=method,
        table=table,
        subscriber_identifier=subscriber_identifier,
        ignore_nulls=ignore_nulls,
        subscriber_subset=subscriber_subset,
    )
