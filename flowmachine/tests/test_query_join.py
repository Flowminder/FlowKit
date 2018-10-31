# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for our custom join API
"""

from unittest import TestCase

import pytest

from flowmachine.core import Table
from flowmachine.core.join import Join
from flowmachine.features import daily_location
from flowmachine.core.query import Query
from flowmachine.core.custom_query import CustomQuery
from pandas import DataFrame


# Define a class that gives us some sample data to join on
class limit(Query):
    def __init__(self, date, offset=0, size=10):
        self.date = date
        self.size = size
        self.offset = offset

        super().__init__()

    def _make_query(self):

        sql = """
        SELECT * FROM
            ( SELECT * FROM ({dl}) AS dl LIMIT {size} OFFSET {offset} ) l
        """.format(
            dl=daily_location(self.date).get_query(), size=self.size, offset=self.offset
        )

        return sql


@pytest.mark.parametrize("join_type", Join.join_kinds)
def test_join_column_names(join_type):
    """Test that join column_names attribute is correct"""
    t = Table("events.calls_20160101")
    joined = t.join(t, on_left="msisdn", how=join_type)
    cols = t.column_names
    cols.remove("msisdn")
    expected = ["msisdn"] + cols + [f"{c}" for c in cols]
    assert expected == joined.column_names


class test_join(TestCase):
    def setUp(self):

        self.dl1 = daily_location("2016-01-01")
        self.dl2 = daily_location("2016-01-02")

        self.stub1 = limit("2016-01-01")
        self.stub2 = limit("2016-01-01", offset=5)

        self.subset_q = CustomQuery("SELECT msisdn FROM events.calls LIMIT 10")

    def _query_has_values(self, Q, expected_values, column="subscriber"):
        """
        Test if the values of a dataframes columns are equal
        to certain values.
        """
        value_set = set(Q.get_dataframe()[column])
        self.assertEqual(set(expected_values), value_set)

    def test_can_join(self):
        """
        Two queries can be joined.
        """

        df = self.dl1.join(self.dl2, on_left="subscriber").get_dataframe()
        self.assertIs(type(df), DataFrame)

    def test_name_append(self):
        """
        Can append a custom name to a join.
        """

        df = self.dl1.join(
            self.dl2, on_left="subscriber", left_append="_left", right_append="_right"
        ).get_dataframe()
        self.assertEqual(list(df.columns), ["subscriber", "name_left", "name_right"])

    def test_value_of_join(self):
        """
        One randomly chosen value is correct
        """

        df = self.dl1.join(
            self.dl2, on_left="subscriber", left_append="_day1", right_append="_day2"
        ).get_dataframe()
        self.assertEqual(
            list(df.set_index("subscriber").ix["ye8jQ0ovnGd9GlJa"]),
            ["Rukum", "Baglung"],
        )

    def test_left_join(self):
        """
        FlowMachine.Join can be done as a left join.
        """

        table = self.stub1.join(
            self.stub2, on_left="subscriber", how="left"
        ).get_dataframe()
        self.assertEqual(len(table), 10)
        self.assertEqual(table.subscriber.isnull().sum(), 0)

    def test_right_join(self):
        """
        FlowMachine.Join can be done as a right join.
        """

        table = self.stub1.join(
            self.stub2, on_left="subscriber", how="right"
        ).get_dataframe()
        self.assertEqual(len(table), 10)
        self.assertEqual(table.subscriber.isnull().sum(), 0)

    def test_left_join(self):
        """
        FlowMachine.Join can be done as a left join.
        """

        table = self.stub1.join(
            self.stub2, on_left="subscriber", how="left"
        ).get_dataframe()
        self.assertEqual(len(table), 10)
        self.assertEqual(table.subscriber.isnull().sum(), 0)

    def test_join_multiple_columns(self):
        """
        flowmachine.Join can be done on more than one column
        """
        pass

    def test_raises_value_error(self):
        """
        flowmachine.Join raises value error when on_left and on_right are different lengths.
        """

        with self.assertRaises(ValueError):
            self.dl1.join(
                self.dl2, on_left=["subscriber", "location_id"], on_right="subscriber"
            )

    def test_using_join_to_subset(self):
        """
        Can we use the join method to subset with a query
        """
        sub = self.dl1.join(self.subset_q, on_left=["subscriber"], on_right=["msisdn"])
        self._query_has_values(sub, self.subset_q.get_dataframe()["msisdn"])
