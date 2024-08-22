# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import warnings

from typing import List, Any

from .query import Query

from psycopg2.extensions import adapt


class UnionWithFixedValues(Query):
    """
    Represents concatenating two tables on top of each other, with an added column
    with a fixed value per table.

    Parameters
    ----------
    queries : List[flowmachine.Query object]
        Queries to union
    fixed_value : List[Any]
        Fixed value to use, converted to postgres type using psycopg2 adapter. Must be the same type.
    fixed_value_column_name : str
        Name of the column to add
    all : bool, default: True
        If False, queries will be deduped, if set to True, duplicate rows will be preserved.
    """

    def __init__(
        self,
        queries: List[Query],
        fixed_value: List[Any],
        fixed_value_column_name: str,
        all: bool = True,
    ):
        self.queries = queries
        self.all = all
        if not len(set(type(val) for val in fixed_value)) == 1:
            raise ValueError("Types of all fixed values must be the same.")
        if not len(fixed_value) == len(queries):
            raise ValueError("Must supply a fixed value for each table/query.")
        self.fixed_value = [f"{adapt(val)}" for val in fixed_value]
        self.fixed_value_column_name = fixed_value_column_name
        column_set = set(tuple(query.column_names) for query in queries)
        if len(column_set) > 1:
            raise ValueError("Inconsistent columns.")
        if len(queries) < 1:
            raise ValueError("Union requires at least one query.")
        if (len(queries) == 1) and (not all):
            warnings.warn("Single queries are not deduped by Union.")

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [*self.queries[0].column_names, self.fixed_value_column_name]

    def _make_query(self):
        return (" UNION ALL " if self.all else " UNION ").join(
            f"(SELECT *, {fixed} AS {self.fixed_value_column_name} FROM ({query.get_query()}) _)"
            for query, fixed in zip(self.queries, self.fixed_value)
        )
