# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from .metaclasses import SubscriberFeature
from flowmachine.features.subscriber.contact_balance import ContactBalance
from flowmachine.utils import Statistic


class PerContactEventStats(SubscriberFeature):
    """
    This class returns the statistics of event count per contact per
    subscriber within the period, optionally limited to only incoming or
    outgoing events. For instance, it calculates the average number of events
    per contact per subscriber.

    Parameters
    ----------
    contact_balance: flowmachine.features.ContactBalance
        An instance of `ContactBalance` which lists the contacts of the
        targeted subscribers along with the number of events between them.
    statistic : Statistic, default Statistic.AVG
        Defaults to avg, aggregation statistic over the durations.

    Examples
    --------

    >>> s = PerContactEventStats("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

          subscriber      value
    J0Yyqw2rkVEwpMG2       13.5
    xkZb5E55LYE10wa4        9.5
    oqNR8gkbv6e4K97z        9.5
    2GJxeNazvlgZbqj6        9.5
    D6b8NwVBmmw5JzA1       11.5
                 ...        ...
    """

    def __init__(
        self,
        contact_balance: ContactBalance,
        statistic: Statistic = Statistic.AVG,
    ):
        self.contact_balance = contact_balance
        self.statistic = Statistic(statistic.lower())

        super(PerContactEventStats, self).__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):

        return f"""
        SELECT subscriber, {self.statistic:events} AS value
        FROM ({self.contact_balance.get_query()}) C
        GROUP BY subscriber
        """
