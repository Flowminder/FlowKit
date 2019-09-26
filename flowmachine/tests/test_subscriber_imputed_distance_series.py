# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import date, datetime

import pytest
from flowmachine.features import DistanceSeries, daily_location, SubscriberLocations

from flowmachine.core import make_spatial_unit

import pandas as pd

# "AZj6MqBAryVyNRDo"
from flowmachine.features.subscriber.imputed_distance_series import (
    ImputedDistanceSeries,
)


def fill_in_dates(df, window, start, stop):
    """
    :param df: input df, assumed to have columns ['subscriber', 'date', 'dist_to_hl']
    :param window: window size for calculating rolling median
    :return: df with 'missing' dates filled in. Also adds a 'days since last call' and 'rolling_median' column
    """
    df.set_index(df["datetime"].apply(lambda x: pd.to_datetime(x).date()), inplace=True)

    df["days_since_last_call"] = df["datetime"].diff()
    df["days_since_last_call"] = df["days_since_last_call"].apply(lambda x: x.days)

    df["rolling_median"] = df.rolling(window)["value"].median()
    df["rolling_median"].fillna(
        method="backfill", inplace=True
    )  # This is just to fill in the first (window-1) entries

    date_range = pd.date_range(start, stop, closed="left")
    df = df.reindex(index=date_range)  # Fill in missing dates
    df["datetime"] = df.index
    df.reset_index(inplace=True)
    df.drop("index", axis=1, inplace=True)

    df["subscriber"].fillna(method="ffill", inplace=True)
    df["rolling_median"].fillna(
        method="ffill", inplace=True
    )  # Fill in missing medians with the previous value
    df["rolling_median"].fillna(
        method="backfill", inplace=True
    )  # Fill in missing medians with the previous value
    df["value"].fillna(
        df["rolling_median"], inplace=True
    )  # Fill in missing value entries with the previous medians
    df["days_since_last_call"].fillna(
        method="backfill", inplace=True
    )  # BACKfill here so that we have a record of the maximum
    # uncertainty of the timing of a step
    return df


def test_impute(get_dataframe):
    sl = SubscriberLocations(
        "2016-01-01",
        "2016-01-07",
        spatial_unit=make_spatial_unit("lon-lat"),
        hours=(20, 0),
    )
    ds = DistanceSeries(subscriber_locations=sl, statistic="min")
    ds_df = get_dataframe(ds)
    sql = get_dataframe(ImputedDistanceSeries(distance_series=ds))
    all_subs = ds_df.subscriber.drop_duplicates()
    for sub in all_subs:
        print(sub)
        if ds_df[ds_df.subscriber == sub].datetime.nunique() > 3:
            to_be_imputed = ds_df[ds_df.subscriber == sub].sort_values("datetime")
            imputed = fill_in_dates(to_be_imputed, 3, sl.start, sl.stop)
            assert (
                imputed.value.values.tolist()
                == sql[sql.subscriber == sub].value.tolist()
            )


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("min", 0, 0),
        ("median", 426.73295117925, 407.89567243203),
        ("stddev", 259.637731932506, 193.169444586714),
        ("avg", 298.78017265186, 375.960833781832),
        ("sum", 896.34051795558, 4511.53000538199),
        ("variance", 67411.7518430558, 37314.4343219396),
    ],
)
def test_returns_expected_values(stat, sub_a_expected, sub_b_expected, get_dataframe):
    """
    Test that we get expected return values for the various statistics
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    rl = daily_location("2016-01-01", spatial_unit=make_spatial_unit("lon-lat"))
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location=rl,
            statistic=stat,
        )
    ).set_index(["subscriber", "datetime"])
    assert df.loc[(sub_a_id, date(2016, 1, 1))].value == pytest.approx(sub_a_expected)
    assert df.loc[(sub_b_id, date(2016, 1, 6))].value == pytest.approx(sub_b_expected)


@pytest.mark.parametrize(
    "stat, sub_a_expected, sub_b_expected",
    [
        ("min", 9284030.27181416, 9123237.1676943),
        ("median", 9327090.49789517, 9348965.25483016),
        ("stddev", 213651.966918163, 180809.405030127),
        ("avg", 9428284.48898054, 9390833.62618702),
        ("sum", 28284853.4669416, 112690003.514244),
        ("variance", 45647162968.0, 32692040947.3485),
    ],
)
def test_returns_expected_values_fixed_point(
    stat, sub_a_expected, sub_b_expected, get_dataframe
):
    """
    Test that we get expected return values for the various statistics with 0, 0 reference
    """
    sub_a_id, sub_b_id = "j6QYNbMJgAwlVORP", "NG1km5NzBg5JD8nj"
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            statistic=stat,
        )
    ).set_index(["subscriber", "datetime"])
    assert df.loc[(sub_a_id, date(2016, 1, 1))].value == pytest.approx(sub_a_expected)
    assert df.loc[(sub_b_id, date(2016, 1, 6))].value == pytest.approx(sub_b_expected)


def test_no_cast_for_below_day(get_dataframe):
    """
    Test that results aren't cast to date for smaller time buckets.
    """
    df = get_dataframe(
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
            ),
            time_bucket="hour",
        )
    )
    assert isinstance(df.datetime[0], datetime)


def test_error_when_subs_locations_not_point_geom():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("admin3")
            )
        )


def test_error_on_spatial_unit_mismatch():
    """
    Test that error is raised if the spatial unit of the subscriber locations isn't point.
    """

    rl = daily_location("2016-01-01")

    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location=rl,
        )


def test_invalid_statistic_raises_error():
    """
    Test that passing an invalid statistic raises an error.
    """
    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            statistic="NOT_A_STATISTIC",
        )


def test_invalid_time_bucket_raises_error():
    """
    Test that passing an invalid time bucket raises an error.
    """
    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            time_bucket="NOT_A_BUCKET",
        )


def test_reference_raises_error():
    """
    Test that passing an invalid reference location raises an error.
    """
    with pytest.raises(ValueError):
        DistanceSeries(
            subscriber_locations=SubscriberLocations(
                "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
            ),
            reference_location="NOT_A_LOCATION",
        )
