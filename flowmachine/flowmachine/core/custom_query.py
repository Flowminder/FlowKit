# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Simple utility class that allows the user to define their
own custom query via a python string.
"""
from typing import List, Set, Union

from flowmachine.utils import pretty_sql
from .query import Query


class CustomQuery(Query):
    """
    Gives the use an interface to create any custom query by simply passing a
    full sql query.

    Parameters
    ----------
    sql : str
        An sql query string
    column_names : list of str or set of str
        The column names to return

    Examples
    --------

    >>> CQ = CustomQuery('SELECT * FROM events.calls', ["msisdn"])
    >>> CQ.head()


    See Also
    --------
    .table.Table for an equivalent that deals with simple table access
    """

    def __init__(
        self, sql: str, column_names: Union[List[str], Set[str]], queries=None
    ):
        self.queries = dict() if queries is None else queries
        hashing_sql = sql.format(
            **{
                k: f"SELECT {v.column_names_as_string_list} FROM {k}"
                for k, v in self.queries.items()
            }
        )
        self.sql = pretty_sql(hashing_sql)

        seen = {}  # Dedupe the column names but preserve order
        self._column_names = [
            seen.setdefault(x, x) for x in column_names if x not in seen
        ]
        super().__init__()
        self.sql = sql

    @property
    def column_names(self) -> List[str]:
        return self._column_names

    def _make_query(self):
        return self.sql.format(
            {name: query.get_query() for name, query in self.queries.items()}
        )
