# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Simple utility class that allows the user to define their
own custom query via a python string.
"""
import re

from typing import List, Set, Union, Dict

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
        self,
        sql: str,
        column_names: Union[List[str], Set[str]],
        **queries: Query,
    ):
        self.queries = queries
        # Standardise the SQL, but then swap back in tokenized queries for runtime
        # and generating the query ID
        hashing_sql = sql.format(
            **{k: f"SELECT * FROM x{v.query_id}" for k, v in self.queries.items()}
        )
        sql = pretty_sql(hashing_sql)
        for q_name, qur in self.queries.items():
            sql = re.sub(f"SELECT\s+\*\s+FROM\s+x{qur.query_id}", qur.tokenize(), sql)
        self.sql = sql
        seen = {}  # Dedupe the column names but preserve order
        self._column_names = [
            seen.setdefault(x, x) for x in column_names if x not in seen
        ]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self._column_names

    def _make_query(self):
        return self.sql
