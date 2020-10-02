# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.utils import pretty_sql

from flowmachine.core import CustomQuery, make_spatial_unit
from flowmachine.features import daily_location


def test_daily_location_1_sql(diff_reporter):
    """
    Simple daily location query returns the expected SQL string.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_1_df(get_dataframe, diff_reporter):
    """
    Simple daily location query returns the expected data.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_daily_location_2_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected SQL string.
    """
    dl = daily_location(
        "2016-01-04",
        spatial_unit=make_spatial_unit(
            "admin", level=2, region_id_column_name="admin2pcod"
        ),
        hours=(3, 9),
        method="most-common",
        subscriber_identifier="imei",
        ignore_nulls=False,
        subscriber_subset=[
            "2GJxeNazvlgZbqj6",
            "7qKmzkeMbmk5nOa0",
            "8dpPLR15XwR7jQyN",
            "1NqnrAB9bRd597x2",
        ],
    )
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_2_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    dl = daily_location(
        "2016-01-04",
        spatial_unit=make_spatial_unit("admin", level=2),
        hours=(3, 9),
        method="most-common",
        # subscriber_identifier="imei",
        ignore_nulls=False,
        subscriber_subset=[
            "2GJxeNazvlgZbqj6",
            "7qKmzkeMbmk5nOa0",
            "8dpPLR15XwR7jQyN",
            "1NqnrAB9bRd597x2",
        ],
    )
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_daily_location_3_sql(diff_reporter):
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
        # subscriber_identifier="imei",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_3_df(get_dataframe, diff_reporter):
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
        # subscriber_identifier="imei",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_daily_location_4_sql(diff_reporter):
    """
    Regression test; this verifies the SQL statement for the test below (which checks the resulting dataframe)
    """
    subset_query = CustomQuery(
        "SELECT * FROM (VALUES ('dr9xNYK006wykgXj')) as tmp (subscriber)",
        column_names=["subscriber"],
    )
    dl = daily_location(
        "2016-01-05",
        table="events.calls",
        hours=(22, 6),
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_4_df(get_dataframe, diff_reporter):
    """
    Regression test; the expected result is empty because the given subscriber does not make any calls on the given date.
    """
    subset_query = CustomQuery(
        "SELECT * FROM (VALUES ('dr9xNYK006wykgXj')) as tmp (subscriber)",
        ["subscriber"],
    )
    dl = daily_location(
        "2016-01-05",
        table="events.calls",
        hours=(22, 6),
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_daily_location_5_sql(diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery(
        "SELECT DISTINCT msisdn AS subscriber FROM events.calls WHERE msisdn in ('GNLM7eW5J5wmlwRa', 'e6BxY8mAP38GyAQz', '1vGR8kp342yxEpwY')",
        ["subscriber"],
    )
    dl = daily_location(
        "2016-01-05",
        spatial_unit=make_spatial_unit("cell"),
        hours=(23, 5),
        method="last",
        # subscriber_identifier="imei",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_5_df(get_dataframe, diff_reporter):
    """
    Daily location query with non-default parameters returns the expected data.
    """
    subset_query = CustomQuery(
        """
        SELECT DISTINCT msisdn AS subscriber
        FROM events.calls
        WHERE (   (datetime >= '2016-01-01 08:00:00' AND datetime <= '2016-01-01 20:00:00' AND substring(tac::TEXT, 0, 2) = '68')
               OR (datetime >= '2016-01-07 14:00:00' AND datetime <= '2016-01-07 15:00:00' AND duration < 400))
        """,
        ["subscriber"],
    )

    dl = daily_location(
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        hours=(4, 9),
        method="most-common",
        # subscriber_identifier="imei",
        # ignore_nulls=False,
        subscriber_subset=subset_query,
    )
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_daily_location_6_sql(diff_reporter):
    """
    Regression test; this verifies the SQL statement for the test below (which checks the resulting dataframe)
    """
    subset_query = CustomQuery(
        """
        SELECT outgoing, datetime, duration, msisdn AS subscriber
        FROM events.calls
        WHERE datetime::date = '2016-01-01' AND duration > 2000
        """,
        ["subscriber"],
    )
    dl = daily_location("2016-01-03", table="calls", subscriber_subset=subset_query)
    sql = pretty_sql(dl.get_query())
    diff_reporter(sql)


def test_daily_location_6_df(get_dataframe, diff_reporter):
    """
    Regression test; the expected result is empty because the given subscriber does not make any calls on the given date.
    """
    subset_query = CustomQuery(
        """
        SELECT outgoing, datetime, duration, msisdn AS subscriber
        FROM events.calls
        WHERE datetime::date = '2016-01-01' AND duration > 2000
        """,
        ["outgoing", "datetime", "duration", "subscriber"],
    )
    dl = daily_location("2016-01-03", table="calls", subscriber_subset=subset_query)
    df = get_dataframe(dl)
    diff_reporter(df.to_csv())


def test_versioned_site_sql(diff_reporter):
    """
    Verify the SQL for a versioned-site spatial unit.
    """
    su = make_spatial_unit("versioned-site")
    sql = pretty_sql(su.get_query())
    diff_reporter(sql)
