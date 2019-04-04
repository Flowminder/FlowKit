# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.utils import pretty_sql

from approvaltests.approvals import verify
from flowmachine.core import CustomQuery
from flowmachine.features import EventCount


def test_event_count_1_sql(diff_reporter):
    """
    Event count query with non-default parameters returns the expected data.
    """
    ec = EventCount(start="2016-01-02", stop="2016-01-04", tables=None)
    sql = pretty_sql(ec.get_query())
    verify(sql, diff_reporter)


def test_event_count_1_df(get_dataframe, diff_reporter):
    """
    Event count query with non-default parameters returns the expected data.
    """
    ec = EventCount(start="2016-01-02", stop="2016-01-04", tables=None)
    df = get_dataframe(ec)
    verify(df.to_csv(), diff_reporter)
