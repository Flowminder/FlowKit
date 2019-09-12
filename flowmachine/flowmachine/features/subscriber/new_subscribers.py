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
    unique_subscribers_benchmark : UniqueSubscribers
        Query returning the unique subscribers in the benchmark period
    unique_subscribers_focal : UniqueSubscribers
        Query returning the unique subscribers in the focal period

    Examples
    --------
    >>>newbies = NewSubscribers(unique_subscribers_bench_mark=UniqueSubscribers('2016-01-01','2016-01-07'), unique_subscribers_focal=UniqueSubscribers('2016-03-01', '2016-03-07'))
    subscriber | location
    ---------------
      A  |  cellX
      B  |  cellY
      Z  |  cellW


    See Also
    --------
    flowmachine.utilities.sets.UniqueSubscribers
    """

    def __init__(
        self,
        *,
        unique_subscribers_bench_mark: UniqueSubscribers,
        unique_subscribers_focal: UniqueSubscribers,
    ):

        if (
            unique_subscribers_bench_mark.subscriber_identifier
            == unique_subscribers_focal.subscriber_identifier
        ):
            self.unique_subscribers_bench_mark = unique_subscribers_bench_mark
            self.unique_subscribers_focal = unique_subscribers_focal
        else:
            raise ValueError(
                f"Mismatched subscriber_identifier: benchmark is '{unique_subscribers_bench_mark.subscriber_identifier}' but focal is '{unique_subscribers_focal.subscriber_identifier}'"
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
