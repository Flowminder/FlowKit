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
    df["value"].fillna(df["rolling_median"], inplace=True)
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
    "size, match", [(1, "positive"), (0, "positive"), (-1, "positive"), (4, "odd")]
)
def test_bad_window(size, match):
    """
    Test some median unfriendly window sizes raise errors.
    """
    with pytest.raises(ValueError, match=match):
        ImputedDistanceSeries(None, window_size=size)
