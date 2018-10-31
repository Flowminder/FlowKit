# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Simple utility class that allows the user to define their
own custom query via a python string.
"""
from .utils import pretty_sql
from .query import Query


class CustomQuery(Query):
    """
    Gives the use an interface to create any custom query by simply passing a
    full sql query.

    Parameters
    ----------
    sql : str
        An sql query string
    
    Examples
    --------

    >>> CQ = CustomQuery('SELECT * FROM events.calls')
    >>> CQ.head()


    See Also
    --------
    .table.Table for an equivalent that deals with simple table access
    """

    def __init__(self, sql):
        """

        """

        self.sql = pretty_sql(sql)
        super().__init__()

    def _make_query(self):
        return self.sql
