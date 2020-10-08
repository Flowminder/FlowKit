# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from .query import Query


class Union(Query):
    """
    Represents concatenating two tables on top of each other with duplicates removed,
    exactly as the postgres UNION.

    Parameters
    ----------
    top, bottom : flowmachine.Query object
        First and second tables respectively
    """

    def __init__(self, top, bottom, all=True):
        """"""

        self.top = top
        self.bottom = bottom
        self.all = all

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return list(self.top.column_names)

    def _make_query(self):

        return """({}) UNION {} ({})""".format(
            self.top.get_query(), "ALL" if self.all else "", self.bottom.get_query()
        )
