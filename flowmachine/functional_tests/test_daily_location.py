# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from approvaltests.approvals import verify
from flowmachine.features import daily_location


def test_simple_daily_location_sql(diff_reporter):
    """
    daily_location() is equivalent to the MostFrequentLocation().
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    sql = dl.get_query()
    verify(sql, diff_reporter)


def test_simple_daily_location_df(get_dataframe, diff_reporter):
    """
    daily_location() is equivalent to the MostFrequentLocation().
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)
