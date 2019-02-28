# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Identifies the first location in which a subscriber
is seen within a specified time period.



"""
from typing import List

from flowmachine.utils import get_columns_for_level
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import subscriber_locations


class FirstLocation(SubscriberFeature):
    """
    Class that defines the time at which the subscriber was first spotted
    at the location in question. Note this does not imply that the
    subscriber was never here before, but this is the first time within
    the specified time range.

    Parameters
    ----------
    start : str
        String representing the beginning of the focal time period
    stop : str
        String representing the end of the focal period
    location : str, tuple, list of str, or list of tuple
        str representing the location of interest. Could be
        a cell or an admin region for instance. You must specify
        level to match this. i.e. location='ER0980', level='cell'.
        Can also pass a list of strings e.g. ['ER0980', 'CW2020']
        will return the time at which the subscriber was first at any
        of these locations. Pass the argument 'any', to find the
        first time a subscriber pops up at any location.
        For the levels versioned-cell, versioned-site may be a tuple or list thereof.
        For the level lat-lon, this _must_ be a tuple.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    See Also
    --------
    flowmachine.features.subscriber_locations
    """

    def __init__(
        self,
        start,
        stop,
        *,
        location,
        level="cell",
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        column_name=None,
        subscriber_subset=None,
        polygon_table=None,
        size=None,
        radius=None,
    ):
        """


        """

        if location == "any" and level != "cell":
            raise ValueError(
                "Invalid parameter combination: location='any' can only be used with level='cell'."
            )

        self.start = start
        self.stop = stop
        self.location = location

        self.ul = subscriber_locations(
            self.start,
            self.stop,
            level=level,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            column_name=column_name,
            subscriber_subset=subscriber_subset,
            polygon_table=polygon_table,
            size=size,
            radius=radius,
        )

        self.table = self.ul.table
        self.subscriber_identifier = self.ul.subscriber_identifier

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "time"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        column_name = get_columns_for_level(self.ul.level)

        clause = self._get_locations_clause(self.location, column_name)

        sql = """
        SELECT 
            relevant_locs.subscriber,
            min(time) AS time
        FROM
            (SELECT * FROM ({subscriber_locs}) AS subscriber_locs
            {clause}) AS relevant_locs
        GROUP BY relevant_locs.subscriber
        """.format(
            subscriber_locs=self.ul.get_query(), clause=clause
        )

        return sql

    def _get_locations_clause(self, location, column_name):
        """
        Private method for getting location clause
        in statement.
        """
        if location == "any":
            return ""
        if len(column_name) == 1:  # polygon, admin, cell, grid
            if isinstance(location, tuple) or isinstance(location, list):
                in_list = "('" + "','".join(location) + "')"
                return "WHERE {} in {}".format(column_name[0], in_list)
            else:
                return "WHERE {} = '{}'".format(column_name[0], location)
        elif self.ul.level == "lat-lon":
            if isinstance(location, tuple) or isinstance(location, list):
                in_list = (
                    "('"
                    + "','".join(
                        "ST_SetSRID(ST_Point({}, {}), 4326)".format(lon, lat)
                        for lon, lat in location
                    )
                    + "')"
                )
                return "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) in {}".format(
                    in_list
                )
            else:
                return "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point({}, {}), 4326)'".format(
                    *location
                )
        else:  # Versioned things
            if isinstance(location, str):  # Deal with single string
                location = (location,)
            elif isinstance(
                location, list
            ):  # Deal with possible single strings in list
                location = [l if isinstance(l, tuple) else (l,) for l in location]
            if isinstance(location, tuple):
                return "WHERE " + " AND ".join(
                    "{} = '{}'".format(c, l) for c, l in zip(column_name, location)
                )
            else:
                ands = " OR ".join(
                    "({})".format(
                        " AND ".join(
                            "{} = '{}'".format(c, l) for c, l in zip(column_name, loc)
                        )
                    )
                    for loc in location
                )
                return "WHERE " + ands
