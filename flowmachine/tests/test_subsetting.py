# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests the check the queries subset method.
"""
import pandas as pd
import pytest

from flowmachine.features import daily_location, RadiusOfGyration
from flowmachine.core import Table


def test_can_numsubset_with_low_and_high(get_dataframe):
    """
    flowmachine.RadiusOfGyration can be subset within a range
    """
    rog = RadiusOfGyration("2016-01-01", "2016-01-02")
    low = 150
    high = 155
    rog_df = (
        get_dataframe(rog)
        .query("{low} <= value <= {high}".format(low=low, high=high))
        .set_index("subscriber")
    )
    sub = get_dataframe(rog.numeric_subset(col="value", low=low, high=high)).set_index(
        "subscriber"
    )

    pd.testing.assert_frame_equal(sub, rog_df)


def test_can_numsubset_with_inf(get_dataframe):
    """
    flowmachine.RadiusOfGyration can be subset between -Inf and Inf
    """
    rog = RadiusOfGyration("2016-01-01", "2016-01-02")
    low = -float("Infinity")
    high = float("Infinity")
    sub = get_dataframe(rog.numeric_subset(col="value", low=low, high=high))
    df = get_dataframe(rog).query("{low} <= value <= {high}".format(low=low, high=high))
    pd.testing.assert_frame_equal(sub, df)


def test_call_with_str_raises_error():
    """
    Numeric subset can't be called with a string in arguments low and high
    """
    rog = RadiusOfGyration("2016-01-01", "2016-01-02")

    with pytest.raises(TypeError):
        rog.numeric_subset(col="value", low="foo", high=1)
    with pytest.raises(TypeError):
        rog.numeric_subset(col="value", low=1, high="bar")


def test_num_subset_can_be_stored(get_dataframe):
    """
    Test that flowmachine.NumericSubset can be stored.
    """
    rog = RadiusOfGyration("2016-01-01", "2016-01-02")
    low = 150
    high = 155
    rog_df = get_dataframe(rog).query(
        "{low} <= value <= {high}".format(low=low, high=high)
    )
    sub = rog.numeric_subset(col="value", low=low, high=high)
    sub.store().result()
    assert sub.is_stored
    # Test that the store is of the right length
    sub = rog.numeric_subset(col="value", low=low, high=high)
    assert len(get_dataframe(sub)) == len(rog_df)


def test_can_subset_with_list_of_subscribers(get_dataframe):
    """
    flowmachine.daily_location can be subset with a list of subscribers.
    """
    dl = daily_location("2016-01-03")
    subscriber_list = list(get_dataframe(dl).head(8).subscriber)
    sub = get_dataframe(dl.subset(col="subscriber", subset=subscriber_list))
    assert set(subscriber_list) == set(sub.subscriber)


def test_can_subset_with_single_subscriber(get_dataframe):
    """
    flowmachine.daily_location can be subset with a single subscriber.
    """
    dl = daily_location("2016-01-03")
    single_subscriber = list(get_dataframe(dl).head(8).subscriber)[3]
    sub = get_dataframe(dl.subset(col="subscriber", subset=single_subscriber))
    assert set(sub.subscriber) == {single_subscriber}


def test_can_subset_with_list_containing_single_subscriber(get_dataframe):
    """
    flowmachine.daily_location can be subset with a list containing a single subscriber.
    """
    dl = daily_location("2016-01-03")
    single_subscriber = list(get_dataframe(dl).head(8).subscriber)[3]
    sub = get_dataframe(dl.subset(col="subscriber", subset=[single_subscriber]))
    assert set(sub.subscriber) == {single_subscriber}


def test_can_getitem_location(get_dataframe):
    """
    flowmachine.daily_location can use get_item
    """
    dl = daily_location("2016-01-03")
    single_subscriber = list(get_dataframe(dl).head(8).subscriber)[3]
    sub = get_dataframe(dl[single_subscriber])
    assert set(sub.subscriber) == {single_subscriber}


def test_special_chars(get_dataframe):
    """Special characters don't break subsets"""
    dl = daily_location("2016-01-03")
    sub = dl.subset(col="subscriber", subset=["Murray'"])
    get_dataframe(sub)
    sub = dl.subset(col="subscriber", subset=["Murray'", "Horace"])
    get_dataframe(sub)
    sub = dl.subset(col="subscriber", subset="Murray'")
    get_dataframe(sub)
    sub = dl.subset(col="subscriber", subset="Murray'")
    get_dataframe(sub)


def test_can_get_item_subscriber_metric(get_dataframe):
    """g
    flowmachine.SubscriberFeature allows for getting items
    """
    rog = RadiusOfGyration("2016-01-01", "2016-01-03")
    dl = daily_location("2016-01-03")
    single_subscriber = list(get_dataframe(dl).head(8).subscriber)[3]
    sub = get_dataframe(rog[single_subscriber])
    assert set(sub.subscriber) == {single_subscriber}


def test_calling_parent_does_not_break_subsetting(get_dataframe):
    """
    flowmachine.Subset doesn't inherit dataframe from parent
    """

    # In the past I have run into this error, so I'll test for
    # it explicitly. If the thing that gets subset has already
    # had _df stored, then if we naively inherited everything
    # then we would get the parent dataframe when asking for it
    dl = daily_location("2016-01-03")
    subscriber_list = list(get_dataframe(dl).head(8).subscriber)
    dl_sub = dl[subscriber_list]
    assert len(dl_sub) == len(subscriber_list)
    assert len(get_dataframe(dl_sub)) == len(subscriber_list)


def test_can_be_stored(get_dataframe):
    """
    Test that flowmachine.Subset can be stored.
    """
    dl = daily_location("2016-01-03")
    subscriber_list = list(get_dataframe(dl).head(8).subscriber)
    sub = dl.subset(col="subscriber", subset=subscriber_list)
    sub.store().result()
    assert sub.is_stored
    # Test that the store is of the right length
    sub = dl.subset(col="subscriber", subset=subscriber_list)
    assert len(sub) == len(subscriber_list)


def test_subset_subset(get_dataframe):

    """
    This test applies two non-numeric subsets one
    after the other .
    """

    sub_cola = "admin1name"
    sub_vala = "Central Development Region"
    sub_colb = "admin2name"
    sub_valb = "Bagmati"
    t = Table("geography.admin3", columns=[sub_cola, sub_colb])
    t_df = get_dataframe(t)

    sub_q = t.subset(sub_cola, sub_vala).subset(sub_colb, sub_valb)
    sub_df = t_df[(t_df[sub_cola] == sub_vala) & (t_df[sub_colb] == sub_valb)]
    sub_df = sub_df.reset_index(drop=True)

    assert get_dataframe(sub_q).equals(sub_df)


def test_subset_subsetnumeric(get_dataframe):
    """
    This test applies a non-numeric subsets and
    a numeric subset one after another in both possible
    orders.
    """

    sub_cola = "admin1name"
    sub_vala = "Central Development Region"
    sub_colb = "shape_area"
    sub_lowb = 0.1
    sub_highb = 0.12
    t = Table("geography.admin3", columns=[sub_cola, sub_colb])
    t_df = get_dataframe(t)

    sub_q1 = t.subset(sub_cola, sub_vala).numeric_subset(sub_colb, sub_lowb, sub_highb)
    sub_q2 = t.numeric_subset(sub_colb, sub_lowb, sub_highb).subset(sub_cola, sub_vala)
    sub_df = t_df[
        (t_df[sub_cola] == sub_vala)
        & (sub_lowb <= t_df[sub_colb])
        & (t_df[sub_colb] <= sub_highb)
    ]
    sub_df = sub_df.reset_index(drop=True)

    assert get_dataframe(sub_q1).equals(sub_df)
    assert get_dataframe(sub_q2).equals(sub_df)


def test_subsetnumeric_subsetnumeric(get_dataframe):

    """
    This test applies two numeric subsets one
    after the other
    """

    sub_cola = "shape_star"
    sub_lowa = 0.1
    sub_higha = 0.2
    sub_colb = "shape_leng"
    sub_lowb = 1.0
    sub_highb = 2.0
    t = Table("geography.admin3", columns=[sub_cola, sub_colb])
    t_df = get_dataframe(t)

    sub_q = t.numeric_subset(sub_cola, sub_lowa, sub_lowb).numeric_subset(
        sub_colb, sub_lowb, sub_highb
    )
    sub_df = t_df[
        (sub_lowa <= t_df[sub_cola])
        & (t_df[sub_cola] <= sub_higha)
        & (sub_lowb <= t_df[sub_colb])
        & (t_df[sub_colb] <= sub_highb)
    ]
    sub_df = sub_df.reset_index(drop=True)

    assert get_dataframe(sub_q).equals(sub_df)
