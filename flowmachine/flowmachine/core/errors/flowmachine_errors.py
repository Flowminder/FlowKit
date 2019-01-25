# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Custom errors raised by flowmachine.
"""


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

class MissingDirectionColumnError(Exception):
    """
    Raised when instantiating a class that requires a directed calculation but
    for which the direction column is missing for any of the tables requested.
    """

    def __init__(self, tables):
        msg = "Direction is missing for {tables}"
        Exception.__init__(self, msg)
