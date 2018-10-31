# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest import TestCase

from flowmachine.core import Table
from flowmachine.core.custom_query import CustomQuery


def test_union_column_names():
    """Test that Union's column_names property is accurate"""
    union = Table("events.calls_20160101").union(Table("events.calls_20160102"))
    assert union.head(0).columns.tolist() == union.column_names


class test_query_union(TestCase):
    def setUp(self):
        self.q1 = CustomQuery("SELECT * FROM events.calls LIMIT 10")

    def test_union_all(self):
        """
        Test union with all = True
        """
        union_all = self.q1.union(self.q1)
        union_all_df = union_all.get_dataframe()
        single_id = union_all_df[union_all_df.id == "5wNJA-PdRJ4-jxEdG-yOXpZ"]
        assert len(single_id) == 4

    def test_union(self):
        """
        Test union with all = False
        """
        union = self.q1.union(self.q1, all=False)
        union_df = union.get_dataframe()
        single_id = union_df[union_df.id == "5wNJA-PdRJ4-jxEdG-yOXpZ"]
        assert len(single_id) == 2
