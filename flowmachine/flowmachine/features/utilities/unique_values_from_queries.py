# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Given a list of queries and a list of columns, returns a table of each unique combination of `column` .
"""

from typing import List, Union

from flowmachine.core.query import Query
from flowmachine.core.errors import MissingColumnsError


class UniqueValuesFromQueries(Query):
    """
    Class representing unique values or combinations of values in selected columns across a query or collection
    of queries. Returns a table of `column_names` columns.

    Parameters
    ----------
    query_list: Union[Query, List[Query]]
        A Query object or list of Query objects, each of which must include the columns in `column_names`
    column_names: Union[str, List[str]]
        A column heading or list of column headings to deduplicate

    """

    def __init__(
        self,
        *,
        query_list: Union[Query, List[Query]],
        column_names: Union[str, List[str]],
    ):
        if isinstance(query_list, Query):
            self.query_list = [query_list]
        else:
            self.query_list = query_list
        if type(column_names) is str:
            self._column_names = [column_names]
        else:
            self._column_names = column_names

        for query in self.query_list:
            if any(name not in query.column_names for name in self._column_names):
                raise MissingColumnsError(query, self._column_names)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self._column_names

    def _make_query(self):
        column_str = ", ".join(self.column_names)

        union_stack = "\nUNION ALL\n".join(
            f"SELECT {column_str} FROM ({query.get_query()}) as tbl"
            for query in self.query_list
        )

        sql = f"""
            SELECT {column_str}
            FROM (
                {union_stack}
            ) AS unioned
            GROUP BY {column_str}
        """
        return sql
