# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
This class calculates the Pareto proportion for a subscriber's interactions -
that fraction of their contacts who account for 80% of their interactions.



"""
from typing import List, Union

from flowmachine.features.subscriber.contact_balance import ContactBalance
from flowmachine.features.subscriber.subscriber_degree import SubscriberDegree
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import standardise_date


class ParetoInteractions(SubscriberFeature):
    """
    Calculates the proportion of a subscriber's contacts who
    account for some proportion of their interactions, ala the
    Pareto Principle.

    Returns a two columns, a subscriber, pareto - the proportion
    of that subscribers contacts who account for the requested
    proportion (0.8, by default) of their interactions in this time period.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : tuple of float, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider calls made, received, or both. Defaults to 'both'.
    exclude_self_calls : bool, default True
        Set to false to *include* calls a subscriber made to themself
    table : str, default 'all'
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    proportion : float
        proportion to track below


    Examples
    --------
    >>> p = ParetoInteractions("2016-01-01", "2016-01-02")
    >>> p.get_dataframe()

             subscriber   value
    0  038OVABN11Ak4W5P     1.0
    1  09NrjaNNvDanD8pk     1.0
    2  0ayZGYEQrqYlKw6g     1.0
    3  0DB8zw67E9mZAPK2     1.0
    4  0Gl95NRLjW2aw8pW     1.0
                    ...     ...
    """

    def __init__(
        self,
        start,
        stop,
        proportion=0.8,
        *,
        direction: Union[str, Direction] = Direction.BOTH,
        tables="all",
        subscriber_identifier="msisdn",
        hours="all",
        exclude_self_calls=False,
        subscriber_subset=None,
    ):

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.direction = Direction(direction)
        self.tables = tables
        self.subscriber_identifier = subscriber_identifier
        self.exclude_self_calls = exclude_self_calls

        if 1 > proportion > 0:
            self.proportion = proportion
        else:
            raise ValueError("{} is not a valid proportion.".format(proportion))

        self.contact_balance = ContactBalance(
            self.start,
            self.stop,
            hours=self.hours,
            tables=self.tables,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            exclude_self_calls=exclude_self_calls,
            subscriber_subset=subscriber_subset,
        )

        self.subscriber_degree = SubscriberDegree(
            self.start,
            self.stop,
            hours=self.hours,
            tables=self.tables,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            exclude_self_calls=self.exclude_self_calls,
            subscriber_subset=subscriber_subset,
        )

        self._cols = ["subscriber", "pareto"]

        super().__init__()

    def _make_query(self):

        target_qur = f"""
        SELECT b.subscriber, ceil(sum(events*{self.proportion})) as target
        FROM ({self.contact_balance.get_query()}) b
        GROUP BY b.subscriber
        """

        cumulative_events = f"""
        SELECT row_number() OVER (
        PARTITION BY c.subscriber ORDER BY events DESC, c.msisdn_counterpart DESC) AS n_contacts,
         c.subscriber, c.msisdn_counterpart, c.events,
          sum(c.events) OVER (
        PARTITION BY c.subscriber ORDER BY events DESC, c.msisdn_counterpart DESC) AS cum_events
        FROM ({self.contact_balance.get_query()}) c
        ORDER BY cum_events DESC
        """

        subscriber_count = f"""
        SELECT DISTINCT ON(cu.subscriber) n_contacts, cu.subscriber as subscriber, (tgt.target - cu.cum_events) as dif FROM
        ({cumulative_events}) cu
        LEFT JOIN
        ({target_qur}) tgt
        ON cu.subscriber = tgt.subscriber
        WHERE (tgt.target - cu.cum_events) <= 0
        ORDER BY cu.subscriber, n_contacts
        """

        sql = f"""
        SELECT uc.subscriber as subscriber, uc.n_contacts/ud.value::FLOAT as value FROM
        ({subscriber_count}) uc
        LEFT JOIN
        ({self.subscriber_degree.get_query()}) ud
        ON uc.subscriber = ud.subscriber
        ORDER BY uc.subscriber
        """

        return sql

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]
