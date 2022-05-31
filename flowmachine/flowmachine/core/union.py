# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import warnings

from typing import List

from .query import Query


class Union(Query):
    """
    Represents concatenating two tables on top of each other with duplicates removed,
    exactly as the postgres UNION.

    Parameters
    ----------
    queries : flowmachine.Query object
        Queries to union
    all : bool, default: True
        If False, queries will be deduped, if set to True, duplicate rows will be preserved.
    """

    def __init__(self, *queries: Query, all: bool = True):
        self.queries = queries
        self.all = all
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
        return list(self.queries[0].column_names)

    def _make_query(self):
        return (" UNION ALL " if self.all else " UNION ").join(
            f"({query.get_query()})" for query in self.queries
        )
