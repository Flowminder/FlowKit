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

from .last_location import LastLocation
from .most_frequent_location import MostFrequentLocation


def locate_subscribers(
    start,
    stop,
    level="admin3",
    hours="all",
    method="last",
    table="all",
    subscriber_identifier="msisdn",
    column_name=None,
    *,
    ignore_nulls=True,
    subscriber_subset=None,
    polygon_table=None,
    size=None,
    radius=None,
):
    """
    Return a class representing the location of an individual. This can be called
    with a number of different methods.

    Find the last/most-frequent location for every subscriber within the given time
    frame. Specify a level.

    Parameters
    ----------
    start, stop : str
        iso format date range for the the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    level : str, default 'admin3'
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
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
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.
    kwargs :
        Eventually passed to flowmachine.spatial_metrics.spatial_helpers.
        JoinToLocation. Here you can specify a non standard set of polygons.
        See the doc string of JoinToLocation for more details.

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
                                 level = 'cell'
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

    if method == "last":
        return LastLocation(
            start,
            stop,
            level,
            hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            column_name=column_name,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
            polygon_table=polygon_table,
            size=size,
            radius=radius,
        )
    elif method == "most-common":
        return MostFrequentLocation(
            start,
            stop,
            level,
            hours,
            table=table,
            column_name=column_name,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
            polygon_table=polygon_table,
            size=size,
            radius=radius,
        )
    # elif self.method == 'first':
    #     _obj = FirstLocation(start, stop, level, hours)
    else:
        raise ValueError(
            f"Unrecognised method '{method}', must be either 'most-common' or 'last'"
        )


def daily_location(
    date,
    stop=None,
    *,
    level="admin3",
    hours="all",
    method="last",
    table="all",
    subscriber_identifier="msisdn",
    column_name=None,
    ignore_nulls=True,
    subscriber_subset=None,
    polygon_table=None,
    size=None,
    radius=None,
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
    level : str, default 'admin3'
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
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
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    """

    # Temporary band-aid; marshmallow deserialises date strings
    # to date objects, so we convert it back here because the
    # lower-level classes still assume we are passing date strings.
    if isinstance(date, datetime.date):
        date = date.strftime("%Y-%m-%d")

    if stop is None:
        # 'cast' the date object as a date
        d1 = datetime.date(*map(int, date.split("-")))
        # One day after this
        d2 = d1 + datetime.timedelta(1)
        stop = d2.strftime("%Y-%m-%d")
    return locate_subscribers(
        start=date,
        stop=stop,
        level=level,
        hours=hours,
        method=method,
        table=table,
        subscriber_identifier=subscriber_identifier,
        column_name=column_name,
        ignore_nulls=ignore_nulls,
        subscriber_subset=subscriber_subset,
        polygon_table=polygon_table,
        size=size,
        radius=radius,
    )
