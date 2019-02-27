# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the proportion of calls that a
subscriber makes during night time. Nocturnal
hour definitions can be specified.



"""
from typing import List

from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import subscriber_locations


class NocturnalCalls(SubscriberFeature):
    """
    Represents the percentage of calls (or other phone events) that
    a subscriber make/receives which are at night. The definition of night
    is configurable.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    hours : tuple of ints, default (20, 4)
        Hours that count as being nocturnal. e.g. (20,4)
        will be the times after 8pm and before 4 am.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    args, kwargs :
        Passed to flowmachine.subscriber_locations
    """

    def __init__(self, start, stop, hours=(20, 4), *args, **kwargs):
        """

        """

        self.start = start
        self.stop = stop
        self.hours = hours
        self.ul = subscriber_locations(
            self.start, self.stop, level="cell", *args, **kwargs
        )
        self.table = self.ul.table
        self.subscriber_identifier = self.ul.subscriber_identifier

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "percentage_nocturnal"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        is_nocturnal = """
        SELECT 
            subscriber_locs.subscriber, 
            time,
            CASE
                WHEN extract(hour FROM time) >= {}
                  OR extract(hour FROM time) < {}
                THEN 1
            ELSE 0
        END AS nocturnal
        FROM ({subscriber_locs}) AS subscriber_locs
        """.format(
            *self.hours, subscriber_locs=self.ul.get_query()
        )

        sql = """
        SELECT 
            nocturnal.subscriber,
            avg(nocturnal.nocturnal)*100 AS percentage_nocturnal
        FROM ({nocturnal}) AS nocturnal
        GROUP BY nocturnal.subscriber
        """.format(
            nocturnal=is_nocturnal
        )

        return sql
