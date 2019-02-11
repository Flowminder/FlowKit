# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.utils import pretty_sql

from approvaltests.approvals import verify
from flowmachine.core import CustomQuery
from flowmachine.features import daily_location


def test_daily_location_1_sql(diff_reporter):
    """
    Simple daily location query returns the expected SQL string.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    sql = pretty_sql(dl.get_query())
    verify(sql, diff_reporter)


def test_daily_location_1_df(get_dataframe, diff_reporter):
    """
    Simple daily location query returns the expected data.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)


def test_daily_location_2_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected SQL string.
    """
    dl = daily_location(
        "2016-01-04",
        level="admin2",
        hours=(3, 9),
        method="most-common",
        subscriber_identifier="imei",
        column_name="admin2pcod",
        ignore_nulls=False,
        subscriber_subset=["2GJxeNazvlgZbqj6", "7qKmzkeMbmk5nOa0", "8dpPLR15XwR7jQyN", "1NqnrAB9bRd597x2"],
    )
    sql = pretty_sql(dl.get_query())
    verify(sql, diff_reporter)


def test_daily_location_2_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    dl = daily_location(
        "2016-01-04",
        level="admin2",
        hours=(3, 9),
        method="most-common",
        #subscriber_identifier="imei",
        #column_name="admin2pcod",
        ignore_nulls=False,
        subscriber_subset=["2GJxeNazvlgZbqj6", "7qKmzkeMbmk5nOa0", "8dpPLR15XwR7jQyN", "1NqnrAB9bRd597x2"],
    )
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)


def test_daily_location_3_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery("SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')")
    dl = daily_location(
        "2016-01-05",
        level="cell",
        hours=(23, 5),
        method="last",
        # subscriber_identifier="imei",
        # column_name="admin2pcod",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    verify(sql, diff_reporter)


def test_daily_location_3_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery("SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')")
    dl = daily_location(
        "2016-01-05",
        level="cell",
        hours=(23, 5),
        method="last",
        # subscriber_identifier="imei",
        # column_name="admin2pcod",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)


def test_daily_location_4_sql(diff_reporter):
    """
    Regression test; this verifies the SQL statement for the test below (which checks the resulting dataframe)
    """
    subset_query = CustomQuery("SELECT * FROM (VALUES ('dr9xNYK006wykgXj')) as tmp (subscriber)")
    dl = daily_location(
        "2016-01-05",
        table="events.calls",
        hours=(22, 6),
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    verify(sql, diff_reporter)


def test_daily_location_4_df(get_dataframe, diff_reporter):
    """
    Regression test; the expected result is empty because the given subscriber does not make any calls on the given date.
    """
    subset_query = CustomQuery("SELECT * FROM (VALUES ('dr9xNYK006wykgXj')) as tmp (subscriber)")
    dl = daily_location(
        "2016-01-05",
        table="events.calls",
        hours=(22, 6),
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)


def test_daily_location_5_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery("SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')")
    dl = daily_location(
        "2016-01-05",
        level="cell",
        hours=(23, 5),
        method="last",
        # subscriber_identifier="imei",
        # column_name="admin2pcod",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    verify(sql, diff_reporter)


def test_daily_location_5_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery("""
        SELECT DISTINCT msisdn AS subscriber
        FROM events.calls
        WHERE (   (datetime >= '2016-01-01 08:00:00' AND datetime <= '2016-01-01 20:00:00' AND substring(tac::TEXT, 0, 2) = '68')
               OR (datetime >= '2016-01-07 14:00:00' AND datetime <= '2016-01-07 15:00:00' AND duration < 400))
        """)

    dl = daily_location(
        "2016-01-02",
        level="admin3",
        hours=(4, 9),
        method="most-common",
        # subscriber_identifier="imei",
        # column_name="admin2pcod",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    verify(df.to_csv(), diff_reporter)
