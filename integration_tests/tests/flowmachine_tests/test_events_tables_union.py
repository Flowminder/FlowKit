# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.utils import pretty_sql

from flowmachine.core import CustomQuery
from flowmachine.features import EventsTablesUnion


def test_events_tables_union_1_sql(diff_reporter):
    """
    EventsTablesUnion returns the expected sql string.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-03",
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    sql = pretty_sql(etu.get_query())
    diff_reporter(sql)


def test_events_tables_union_1_df(diff_reporter, get_dataframe):
    """
    EventsTablesUnion returns the expected data.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-03",
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    df = get_dataframe(etu)
    diff_reporter(df.to_csv())


def test_events_tables_union_2_sql(diff_reporter):
    """
    EventsTablesUnion with explicit hours returns the expected sql string.
    """
    etu = EventsTablesUnion(
        start="2016-01-03",
        stop="2016-01-05",
        hours=(7, 13),
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    sql = pretty_sql(etu.get_query())
    diff_reporter(sql)


def test_events_tables_union_2_df(diff_reporter, get_dataframe):
    """
    EventsTablesUnion with explicit hours returns the expected data.
    """
    etu = EventsTablesUnion(
        start="2016-01-03",
        stop="2016-01-05",
        hours=(7, 13),
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    df = get_dataframe(etu)
    diff_reporter(df.to_csv())


def test_events_tables_union_3_sql(diff_reporter):
    """
    EventsTablesUnion with hours spanning midnight returns the expected sql string.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-04",
        hours=(21, 5),
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    sql = pretty_sql(etu.get_query())
    diff_reporter(sql)


def test_events_tables_union_3_df(diff_reporter, get_dataframe):
    """
    EventsTablesUnion with hours spanning midnight returns the expected data.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-04",
        hours=(21, 5),
        tables=["calls"],
        columns=[
            "datetime",
            "duration",
            "id",
            "location_id",
            "msisdn",
            "msisdn_counterpart",
            "outgoing",
            "tac",
        ],
    )
    df = get_dataframe(etu)
    diff_reporter(df.to_csv())
