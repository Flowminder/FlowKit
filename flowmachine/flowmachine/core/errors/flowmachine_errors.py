# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Custom errors raised by flowmachine.
"""


class StoreFailedException(Exception):
    """
    Exception indicating that a query failed to store.

    Parameters
    ----------
    query_id : str
        Identifier of the query
    """

    def __init__(self, query_id):
        Exception.__init__(self, f"Query '{query_id}' store failed.")


class QueryResetFailedException(Exception):
    """
    Exception indicating that a query failed to reset while being reset
    from another thread or FlowMachine instance.

    Parameters
    ----------
    query_id : str
        Identifier of the query
    """

    def __init__(self, query_id):
        Exception.__init__(self, f"Query '{query_id}' reset failed.")


class QueryErroredException(Exception):
    """
    Exception indicating that a query failed with an error while being run
    from another thread or FlowMachine instance.

    Parameters
    ----------
    query_id : str
        Identifier of the query
    """

    def __init__(self, query_id):
        Exception.__init__(
            self, f"Query '{query_id}' errored while being run elsewhere."
        )


class QueryCancelledException(Exception):
    """
    Exception indicating that a query was cancelled while being run
    from another thread or FlowMachine instance.

    Parameters
    ----------
    query_id : str
        Identifier of the query
    """

    def __init__(self, query_id):
        Exception.__init__(self, f"Query '{query_id}' was cancelled.")


class NameTooLongError(Exception):
    """
    Custom error to pass when a table name is too
    long for postgres to store.
    """

    pass


class NotConnectedError(Exception):
    """Error indicating the database connection is missing."""

    def __init__(self):
        Exception.__init__(
            self, "No connection found. Do you need to call flowmachine.connect()?"
        )


class BadLevelError(Exception):
    """
    Raised when any class is given an error it does not recognise.

    Parameters
    ----------
    level : str
        The bad level.
    allowed_levels : list of str, optional
        List of allowed levels that the user may pass.
    """

    def __init__(self, level, allowed_levels=None):
        msg = "Unrecognised level {}".format(level)
        if allowed_levels is not None:
            msg += ", level must be one of {}".format(allowed_levels)
        Exception.__init__(self, msg)


class MissingDateError(Exception):
    """
    Raised when instantiating a class that points to a date that does not exist
    in the database.

    Parameters
    ----------

    start, stop : str, optional
        Pass a single date to include in the error message.
    """

    def __init__(self, start=None, stop=None):
        msg = "No data for date"
        if start is not None:
            msg += ": {}".format(start)
        if stop is not None:
            msg += " - {}".format(stop)
        Exception.__init__(self, msg)


class MissingColumnsError(Exception):
    def __init__(self, tables_lacking_columns, columns):
        Exception.__init__(
            self, f"Tables {tables_lacking_columns} are missing columns {columns}."
        )
