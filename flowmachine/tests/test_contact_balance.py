# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the ContactsBalance() class.
"""


from flowmachine.features.subscriber import ContactBalance


def test_some_results(get_dataframe):
    """
    TotalSubscriberEvents() returns a dataframe that contains hand-picked results.
    """
    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07"))
    set_df = df.set_index("subscriber")
    assert set_df.loc["bvEWVnZdwJ8Lgkm2"]["proportion"] == 1.000000
    assert set_df.loc["7XebRKr35JMJnq8A"]["events"] == 12
    assert (
        set_df.loc["3XKdxqvyNxO2vLD1"]["msisdn_counterpart"].values[0]
        == "DELmRj9Vvl346G50"
    )


def test_no_result_is_greater_than_one(get_dataframe):
    """
    No results from ContactBalance()['proportion'] is greater than 1.
    """
    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07"))
    results = df[df["proportion"] > 1]
    assert len(results) == 0
