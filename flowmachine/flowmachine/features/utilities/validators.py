# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import Query


def valid_median_window(arg_name: str, value: int):
    """
    Raise a value error if the value provided is not an odd positive integer bigger than 1.
    Parameters
    ----------
    arg_name : str
        Argument name for error message
    value : int
        Value to check

    Returns
    -------
    int
        The validated value

    Raises
    ------
    ValueError
        If the value is even, negative, or less than 3

    """
    if value < 3:
        raise ValueError(f"{arg_name} must be odd and greater than 1.")
    if (value % 2) == 0:
        raise ValueError(f"{arg_name} must be odd.")
    return value


def validate_column_present(arg_name: str, column: str, query: Query, allow_none=True):
    """
    Raises a value error if the column is not in the columns of the query.

    Parameters
    ----------
    arg_name : str
        Argument being validated
    column : str
        Name of the column
    query : Query
        The query to check the column against the columns of
    allow_none : bool, default True
        Set to False to error when the column is None.

    Returns
    -------
    str
        The column name

    Raises
    ------
    ValueError
        If the column is not one of those returned by the query
    """
    if column is None and not allow_none:
        raise ValueError(
            f"Invalid {arg_name}: Cannot be None. Must be one of {query.column_names}"
        )
    elif column is not None and column not in query.column_names:
        raise ValueError(
            f"Invalid {arg_name}: '{column}' not in query's columns. Must be one of {query.column_names}"
        )
    return column
