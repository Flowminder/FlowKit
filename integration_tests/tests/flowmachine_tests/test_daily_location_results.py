# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import CustomQuery, make_spatial_unit
from flowmachine.features import daily_location
from flowmachine.utils import pretty_sql


def test_daily_location_1_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery(
        "SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')",
        column_names=["subscriber"],
    )
    dl = daily_location(
        "2016-01-05",
        spatial_unit=make_spatial_unit("cell"),
        hours=(23, 5),
        method="last",
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_1_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    # Note that subscriber `1vGR8kp342yxEpwY` should be missing from the result
    # because they have no event on 2016-01-05 after 11pm or before 5am.
    subset_query = CustomQuery(
        "SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')",
        column_names=["subscriber"],
    )
    dl = daily_location(
        "2016-01-05",
        spatial_unit=make_spatial_unit("cell"),
        hours=(23, 5),
        method="last",
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())
