# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for our custom join API
"""

import pytest

from flowmachine_core.query_bases.table import Table
from flowmachine_core.utility_queries.join import Join
from flowmachine_core.utility_queries.custom_query import CustomQuery


@pytest.mark.parametrize("join_type", Join.join_kinds)
def test_join_column_names(join_type):
    """Test that join column_names attribute is correct"""
    t = Table("events.calls_20160101", columns=["location_id", "datetime"])
    t2 = Table("infrastructure.cells", columns=["id", "geom_point"])
    joined = t.join(t2, on_left="location_id", on_right="id", how=join_type)

    expected = [
        "location_id" if join_type in ("left", "inner", "left outer") else "id",
        "datetime",
        "geom_point",
    ]
    assert joined.column_names == expected


def test_name_append():
    """
    Can append a custom name to a join.
    """
    test_query = CustomQuery(
        "SELECT 1 as join_col, 2 as value_col", column_names=["join_col", "value_col"]
    )
    df = test_query.join(
        test_query, on_left="join_col", left_append="_left", right_append="_right"
    )
    assert ["join_col", "value_col_left", "value_col_right"] == df.column_names


def test_value_of_join(get_dataframe):
    """
    One randomly chosen value is correct, and that the expected number of rows are returned
    """
    test_query = CustomQuery(
        "SELECT 1 as join_col, 2 as value_col", column_names=["join_col", "value_col"]
    )
    test_query_2 = CustomQuery(
        "SELECT * FROM (VALUES (1, 2, 'a'), (1, 2, 'b'), (3, 4, 'b')) as t(join_col, value_col, value_col_2)",
        column_names=["join_col", "value_col", "value_col_2"],
    )

    df = get_dataframe(
        test_query.join(
            test_query_2, on_left="join_col", left_append="_day1", right_append="_day2"
        )
    )
    assert [2, 2, "b"] == list(df.set_index("join_col").iloc[1])
    assert 2 == len(df)


def test_ambiguity_is_an_error():
    """
    Join raises an error if resulting columns are ambiguous.
    """
    test_query = CustomQuery(
        "SELECT 1 as join_col, 2 as value_col", column_names=["join_col", "value_col"]
    )
    with pytest.raises(ValueError):
        test_query.join(test_query, on_left="subscriber")


def test_left_join(get_dataframe):
    """
    FlowMachine.Join can be done as a left join.
    """

    test_query = CustomQuery(
        "SELECT 1 as join_col, 2 as value_col", column_names=["join_col", "value_col"]
    )
    test_query_2 = CustomQuery(
        "SELECT * FROM (VALUES (1, 2, 'a'), (1, 2, 'b'), (3, 4, 'b')) as t(join_col, value_col, value_col_2)",
        column_names=["join_col", "value_col", "value_col_2"],
    )

    table = get_dataframe(
        test_query.join(test_query_2, on_left="join_col", how="left", left_append="_")
    )
    assert 2 == len(table)
    assert 0 == table.join_col.isnull().sum()


def test_right_join(get_dataframe):
    """
    FlowMachine.Join can be done as a right join.
    """
    test_query = CustomQuery(
        "SELECT 1 as join_col, 2 as value_col", column_names=["join_col", "value_col"]
    )
    test_query_2 = CustomQuery(
        "SELECT * FROM (VALUES (1, 2, 'a'), (1, 2, 'b'), (3, 4, 'b')) as t(join_col, value_col, value_col_2)",
        column_names=["join_col", "value_col", "value_col_2"],
    )

    table = get_dataframe(
        test_query.join(test_query_2, on_left="join_col", how="right", left_append="_")
    )
    assert 3 == len(table)
    assert 0 == table.join_col.isnull().sum()


def test_raises_value_error(test_query):
    """
    flowmachine_core.Join raises value error when on_left and on_right are different lengths.
    """
    with pytest.raises(ValueError):
        test_query.join(
            test_query, on_left=["subscriber", "location_id"], on_right="subscriber"
        )


def test_using_join_to_subset(get_dataframe):
    """
    Should be able to use the join method to subset one query by another
    """
    test_query = CustomQuery(
        "SELECT * FROM (VALUES (1, 2, 'a'), (1, 2, 'b'), (3, 4, 'b')) as t(join_col, value_col, value_col_2)",
        column_names=["join_col", "value_col", "value_col_2"],
    )
    subset_q = CustomQuery("SELECT 3 as join_col", column_names=["join_col"])
    sub = test_query.join(
        subset_q, on_left=["join_col"], on_right=["join_col"], left_append="left"
    )
    value_set = set(get_dataframe(sub).join_col)
    assert set(get_dataframe(subset_q).join_col) == value_set
    assert 1 == len(value_set)
