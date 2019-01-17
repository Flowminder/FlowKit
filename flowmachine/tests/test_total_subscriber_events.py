# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test the total subscriber events class
"""


from flowmachine.features.subscriber import TotalSubscriberEvents


def test_total_subscriber_events_correct(get_dataframe):
    """
    TotalSubscriberEvents() returns a dataframe that contains hand-picked results.
    """
    te = TotalSubscriberEvents("2016-01-01", "2016-01-04")
    df = get_dataframe(te)
    set_df = df.set_index("subscriber")
    assert set_df.loc["1d29oEA95KEzAKlW"][0] == 16
    assert set_df.loc["x8o7D2Ax8Lg5Y0MB"][0] == 18
    assert set_df.loc["yObw75JkAZ0vKRlz"][0] == 5


def test_total_subscriber_events_calls_only(get_dataframe):
    """
    TotalSubscriberEvents() get only those events which are calls.
    """
    df = get_dataframe(
        TotalSubscriberEvents("2016-01-01", "2016-01-04", event_type="calls")
    ).set_index("subscriber")
    assert df.loc["038OVABN11Ak4W5P"][0] == 9


def test_total_subscriber_events_outgoing_only(get_dataframe):
    """
    TotalSubscriberEvents() subsets those calls that are outgoing only.
    """
    df = get_dataframe(
        TotalSubscriberEvents("2016-01-01", "2016-01-04", direction="out")
    ).set_index("subscriber")
    assert df.loc["038OVABN11Ak4W5P"][0] == 4


def test_total_subscriber_events_type_direction(get_dataframe):
    """
    TotalSubscriberEvents() gets only incoming texts.
    """
    df = get_dataframe(
        TotalSubscriberEvents(
            "2016-01-01", "2016-01-04", event_type="sms", direction="in"
        )
    ).set_index("subscriber")
    assert df.loc["038OVABN11Ak4W5P"][0] == 3
