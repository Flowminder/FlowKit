# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Given a list of queries with a 'subscriber' column, returns a 1-column table of unique subscribers.
"""

from typing import List

from flowmachine.core.query import Query


class UniqueSubscribersFromQueries(Query):
    """
    Class representing unique subscribers across a set of queries

    Parameters
    ----------
    query_list: List[Query]
        A list of Query objects, each of which must include a `subscriber` column

    """

    # Review question: Does this need to be 'subscriber' or can we make the column to select a parameter?

    def __init__(self, query_list: List[Query]):
        self.query_list = query_list
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        union_stack = "\nUNION ALL\n".join(
            [
                f"SELECT subscriber FROM ({query.get_query()}) as tbl"
                for query in self.query_list
            ]
        )

        sql = f"""
            SELECT subscriber
            FROM (
                {union_stack}
            ) AS unioned
            GROUP BY subscriber
        """
        return sql
