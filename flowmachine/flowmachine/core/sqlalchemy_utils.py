# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Selectable


def get_sqlalchemy_table_definition(fully_qualified_table_name, *, engine):
    """
    Return sqlalchemy Table object for table with the given name.

    Parameters
    ----------
    fully_qualified_table_name : str
        Fully qualified table name, for example: "events.calls"
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine to use for reading the table information.

    Returns
    -------
    sqlalchemy.Table
    """
    try:
        schema, table_name = fully_qualified_table_name.split(".")
    except ValueError:
        raise ValueError(
            f"Fully qualified table name must be of the form '<schema>.<table>'. Got: {fully_qualified_table_name}"
        )

    metadata = MetaData()
    return Table(table_name, metadata, schema=schema, autoload_with=engine)


def get_sql_string(sqlalchemy_query):
    """
    Return SQL string compiled from the given sqlalchemy query (using the PostgreSQL dialect).

    Parameters
    ----------
    sqlalchemy_query : sqlalchemy.sql.Selectable
        SQLAlchemy query

    Returns
    -------
    str
        SQL string compiled from the sqlalchemy query.
    """
    assert isinstance(sqlalchemy_query, Selectable)
    compiled_query = sqlalchemy_query.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    sql = str(compiled_query)
    return sql


def get_string_representation(sqlalchemy_expr, engine=None):
    """
    Return a string containing a SQL fragment which is compiled from the given sqlalchemy expression.

    Returns
    -------
    str
        String representation of the sqlalchemy expression.
    """
    # assert isinstance(sqlalchemy_expr, ColumnElement)
    return str(sqlalchemy_expr.compile(engine, compile_kwargs={"literal_binds": True}))


def get_query_result_as_dataframe(query, *, engine):
    """
    Run the given sqlalchemy query and return the result as a pandas DataFrame.

    Parameters
    ----------
    query : sqlalchemy.sql.Selectable
        The SQLAlchemy query to run.
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine to use for reading the table information.

    Returns
    -------
    pandas.DataFrame
        Data frame containing the result.
    """
    assert isinstance(query, Selectable)

    with engine.connect() as con:
        result = con.exec_driver_sql(query)

    columns = [c.name for c in query.columns]
    df = pd.DataFrame(result.fetchall(), columns=columns)
    return df


def make_sqlalchemy_column_from_flowmachine_column_description(
    sqlalchemy_table, column_str
):
    """
    Given a sqlalchemy sqlalchemy_table and a string with a column description, return
    the actual sqlalchemy Column object (or a sqlalchemy Label object if
    `column_str` contains an alias such as "<column> AS <alias>".

    Parameters
    ----------
    table : sqlalchemy.Table
        The sqlalchemy_table for which to obtain the column.
    column_str : str
        The column name, optionally describing an alias via

    Returns
    -------
    sqlalchemy.Column or sqlalchemy.sql.elements.Label

    Examples
    --------

        >>> make_sqlalchemy_column_from_flowmachine_column_description(sqlalchemy_table, "msisdn")
        >>> make_sqlalchemy_column_from_flowmachine_column_description(sqlalchemy_table, "msisdn AS subscriber")
    """
    assert isinstance(sqlalchemy_table, Table)
    parts = column_str.split()
    if len(parts) == 1:
        colname = parts[0]
        col = sqlalchemy_table.c[colname]
    elif len(parts) == 3:
        assert parts[1].lower() == "as"
        colname = parts[0]
        label = parts[2]
        col = sqlalchemy_table.c[colname].label(label)
    else:
        raise ValueError(f"Not a valid column expression: '{column_str}'")

    return col
