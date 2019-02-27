# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import Table
from flowmachine.core.custom_query import CustomQuery


def test_union_column_names():
    """Test that Union's column_names property is accurate"""
    union = Table("events.calls_20160101").union(Table("events.calls_20160102"))
    assert union.head(0).columns.tolist() == union.column_names


def test_union_all(get_dataframe):
    """
    Test default union behaviour keeps duplicates.
    """
    q1 = Table(schema="events", name="calls")
    union_all = q1.union(q1)
    union_all_df = get_dataframe(union_all)
    single_id = union_all_df[union_all_df.id == "5wNJA-PdRJ4-jxEdG-yOXpZ"]
    assert len(single_id) == 4


def test_union(get_dataframe):
    """
    Test union with all set to false dedupes.
    """
    q1 = Table(schema="events", name="calls")
    union = q1.union(q1, all=False)
    union_df = get_dataframe(union)
    single_id = union_df[union_df.id == "5wNJA-PdRJ4-jxEdG-yOXpZ"]
    assert len(single_id) == 2
