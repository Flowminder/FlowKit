# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Generates a list of new subscribers who are present
in the dataset in a given time period, but
absent from the dataset in a subsequent 
comparison time period.


"""
from typing import List

from ..utilities.sets import UniqueSubscribers
from ...core.query import Query


class NewSubscribers(Query):
    """
    Find a list of all new subscribers who were not seen within
    a certain date range.

    Parameters
    ----------
    start1 : str
        The beginning of the comparison period
    stop1 : str
        The end of the comparison period
    start2 : str
        The beginning of the focal period
    stop2 : str
        The end of the focal periods
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Examples
    --------
    >>> newbies = NewSubscribers('2016-01-01', '2016-01-07',
                           '2016-03-01', '2016-03-07')
        subscriber | location
        ---------------
          A  |  cellX
          B  |  cellY
          Z  |  cellW

    """

    def __init__(
        self, start1, stop1, start2, stop2, subscriber_identifier="msisdn", **kwargs
    ):
        """

        """

        self.start1 = start1
        self.stop1 = stop1
        self.start2 = start2
        self.stop2 = stop2
        self.subscriber_identifier = subscriber_identifier
        self.unique_subscribers_bench_mark = UniqueSubscribers(
            self.start1,
            self.stop1,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs
        )
        self.unique_subscribers_focal = UniqueSubscribers(
            self.start2,
            self.stop2,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs
        )

        super().__init__()

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        sql = """{}
        EXCEPT
        ({})
        """.format(
            self.unique_subscribers_focal.get_query(),
            self.unique_subscribers_bench_mark.get_query(),
        )

        return sql

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]
