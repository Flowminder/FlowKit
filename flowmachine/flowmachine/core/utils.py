# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility functions that are used only by the core code and are
not therefore user facing.
"""

import logging

from pglast import prettify
from psycopg2.extensions import adapt

logger = logging.getLogger("flowmachine").getChild(__name__)


def pretty_sql(
    sql,
    compact_lists_margin=0,
    split_string_literals_threshold=0,
    special_functions=True,
    comma_at_eoln=True,
):
    """
    Prettify and validate the syntax of an SQL query, using pglast

    Parameters
    ----------
    sql : str
        SQL to prettyify and validate
    compact_lists_margin : int, default 0
        Use compact form for lists shorter than this
    split_string_literals_threshold : int, default 0
        Split strings (in the sql) longer than this threshold
    special_functions : bool, default True
        Translate some special functions to their more commonly used forms
    comma_at_eoln

    Raises
    ------

    pglast.parser.ParseError
        Raises a parse error if the query syntax was bad.

    See Also
    --------
    pglast.prettify: Function wrapped, use this for additional prettification options

    Returns
    -------
    str
        The prettified string.

    """

    return prettify(
        sql,
        compact_lists_margin=compact_lists_margin,
        split_string_literals_threshold=split_string_literals_threshold,
        special_functions=special_functions,
        comma_at_eoln=comma_at_eoln,
    )


def _makesafe(x):
    """
    Function that converts input into a PostgreSQL readable.
    """
    return adapt(x).getquoted().decode()
