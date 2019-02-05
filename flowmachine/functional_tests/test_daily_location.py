# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from approvaltests.approvals import verify
from flowmachine.features import daily_location


def test_daily_location_1_sql(diff_reporter):
    """
    Simple daily location query returns the expected SQL string.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    sql = dl.get_query()
    verify(sql, diff_reporter)


def test_daily_location_1_df(get_dataframe, diff_reporter):
    """
    Simple daily location query returns the expected data.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)
