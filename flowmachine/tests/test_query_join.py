# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for our custom join API
"""


import pytest

from flowmachine.core import Table
from flowmachine.core.join import Join
from flowmachine.features import daily_location
from flowmachine.core.query import Query
from flowmachine.core.custom_query import CustomQuery


# Define a class that gives us some sample data to join on
class TruncatedAndOffsetDailyLocation(Query):
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


def test_name_append():
    """
    Can append a custom name to a join.
    """

    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")

    df = dl1.join(dl2, on_left="subscriber", left_append="_left", right_append="_right")
    assert ["subscriber", "pcod_left", "pcod_right"] == df.column_names


def test_value_of_join(get_dataframe):
    """
    One randomly chosen value is correct, and that the expected number of rows are returned
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")

    df = get_dataframe(
        dl1.join(dl2, on_left="subscriber", left_append="_day1", right_append="_day2")
    )
    assert ["524 4 10 52", "524 3 08 43"] == list(
        df.set_index("subscriber").ix["ye8jQ0ovnGd9GlJa"]
    )
    assert 490 == len(df)


def test_left_join(get_dataframe):
    """
    FlowMachine.Join can be done as a left join.
    """

    stub1 = TruncatedAndOffsetDailyLocation("2016-01-01")
    stub2 = TruncatedAndOffsetDailyLocation("2016-01-01", offset=5)

    table = get_dataframe(stub1.join(stub2, on_left="subscriber", how="left"))
    assert 10 == len(table)
    assert 0 == table.subscriber.isnull().sum()


def test_right_join(get_dataframe):
    """
    FlowMachine.Join can be done as a right join.
    """
    stub1 = TruncatedAndOffsetDailyLocation("2016-01-01")
    stub2 = TruncatedAndOffsetDailyLocation("2016-01-01", offset=5)

    table = get_dataframe(stub1.join(stub2, on_left="subscriber", how="right"))
    assert 10 == len(table)
    assert 0 == table.subscriber.isnull().sum()


def test_raises_value_error():
    """
    flowmachine.Join raises value error when on_left and on_right are different lengths.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    with pytest.raises(ValueError):
        dl1.join(dl2, on_left=["subscriber", "location_id"], on_right="subscriber")


def test_using_join_to_subset(get_dataframe):
    """
    Should be able to use the join method to subset one query by another
    """
    dl1 = daily_location("2016-01-01")
    subset_q = CustomQuery("SELECT msisdn FROM events.calls LIMIT 10")
    sub = dl1.join(subset_q, on_left=["subscriber"], on_right=["msisdn"])
    value_set = set(get_dataframe(sub).subscriber)
    assert set(get_dataframe(subset_q).msisdn) == value_set
    assert 10 == len(value_set)
