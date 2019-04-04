# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.utils import pretty_sql

from approvaltests.approvals import verify
from flowmachine.core import CustomQuery
from flowmachine.features import EventsTablesUnion


def test_events_tables_union_1_sql(diff_reporter):
    """
    EventsTablesUnion returns the expected sql string.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-03",
        tables=["events.calls"],
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
    verify(sql, diff_reporter)


def test_events_tables_union_1_df(diff_reporter, get_dataframe):
    """
    EventsTablesUnion returns the expected data.
    """
    etu = EventsTablesUnion(
        start="2016-01-02",
        stop="2016-01-03",
        tables=["events.calls"],
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
    verify(df.to_csv(), diff_reporter)
