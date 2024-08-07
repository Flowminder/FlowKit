# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime

import pytest
import numpy

from flowmachine.core import Table
from flowmachine.core.union_with_fixed_values import UnionWithFixedValues


def test_union_column_names():
    """Test that Union's column_names property is accurate"""
    union = UnionWithFixedValues(
        [Table("events.calls_20160101"), Table("events.calls_20160102")],
        ["extra_val", "extra_val"],
        fixed_value_column_name="extra_col",
    )
    assert union.head(0).columns.tolist() == union.column_names
    assert union.column_names == [*Table("events.calls_20160101").columns, "extra_col"]


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
    Test union adds extra columns.
    """
    union = UnionWithFixedValues(
        [Table("events.calls_20160101"), Table("events.calls_20160101")],
        ["extra_val", "extra_val_1"],
        fixed_value_column_name="extra_col",
    )
    df = get_dataframe(union)
    assert df["extra_col"].nunique() == 2
    assert set(df["extra_col"].values) == set(["extra_val", "extra_val_1"])


def test_union_date_type(get_dataframe):
    """
    Test union casts types correctly for datetimes.
    """
    union = UnionWithFixedValues(
        [Table("events.calls_20160101"), Table("events.calls_20160101")],
        [datetime.datetime(2016, 1, 1), datetime.datetime(2016, 1, 2)],
        fixed_value_column_name="extra_col",
    )
    df = get_dataframe(union)
    assert df["extra_col"].dtype == numpy.dtype("datetime64[ns]")


def test_union_raises_with_mismatched_extras_types():
    """
    Test that unioning queries with inconsistent types for extra values raises an error.
    """
    with pytest.raises(ValueError):
        UnionWithFixedValues(
            [Table("events.calls_20160101"), Table("events.calls_20160101")],
            [datetime.date(2016, 1, 1), "NOT A DATE"],
            fixed_value_column_name="extra_col",
        )


def test_union_raises_with_not_enough_extras():
    """
    Test that unioning empty list raises an error.
    """
    with pytest.raises(ValueError):
        UnionWithFixedValues(
            [Table("events.calls_20160101"), Table("events.calls_20160101")],
            [datetime.date(2016, 1, 1)],
            fixed_value_column_name="extra_col",
        )
