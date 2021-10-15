# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the ContactsBalance() class.
"""


from flowmachine.features.subscriber import ContactBalance

import pytest


def test_some_results(get_dataframe):
    """
    ContactBalance() returns a dataframe that contains hand-picked results.
    """
    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07"))
    set_df = df.set_index("subscriber")
    assert set_df.loc["bvEWVnZdwJ8Lgkm2"]["proportion"] == 1.000000
    assert set_df.loc["7XebRKr35JMJnq8A"]["events"] == 12
    assert (
        set_df.loc["3XKdxqvyNxO2vLD1"]["msisdn_counterpart"].values[0]
        == "DELmRj9Vvl346G50"
    )
    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07", direction="in"))
    set_df = df.set_index("subscriber")
    assert set_df.loc["bvEWVnZdwJ8Lgkm2"]["proportion"] == 1.000000
    assert set_df.loc["8lo9EgjnyjgKO7vL"]["events"] == 19
    assert set_df.loc["3XKdxqvyNxO2vLD1"]["msisdn_counterpart"] == "7lNP0mDOAK3xKWv4"

    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07", direction="out"))
    set_df = df.set_index("subscriber")
    assert set_df.loc["V1QBpMj0vEwr2PGW"]["proportion"] == 1.000000
    assert set_df.loc["7XebRKr35JMJnq8A"]["events"] == 12
    assert set_df.loc["3XKdxqvyNxO2vLD1"]["msisdn_counterpart"] == "DELmRj9Vvl346G50"


def test_no_result_is_greater_than_one(get_dataframe):
    """
    No results from ContactBalance()['proportion'] is greater than 1.
    """
    df = get_dataframe(ContactBalance("2016-01-01", "2016-01-07"))
    results = df[df["proportion"] > 1]
    assert len(results) == 0


def test_counterpart_subset(get_dataframe):
    """Test that counterparts_subset method gets correct subset."""
    query = ContactBalance("2016-01-01", "2016-01-03")
    cb = get_dataframe(query)
    cs = get_dataframe(query.counterparts_subset())
    assert set(cb.msisdn_counterpart.values) == set(cs.subscriber.values)

    query = ContactBalance("2016-01-01", "2016-01-03")
    cb = get_dataframe(query)
    cs = get_dataframe(query.counterparts_subset(include_subscribers=True))
    assert set(cb.msisdn_counterpart.values).union(cb.subscriber.values) == set(
        cs.subscriber.values
    )

    query = ContactBalance("2016-01-01", "2016-01-03", subscriber_identifier="imei")
    cb = get_dataframe(query)
    cs = get_dataframe(query.counterparts_subset())
    assert set(cb.msisdn_counterpart.values) == set(cs.subscriber.values)

    query = ContactBalance("2016-01-01", "2016-01-03", subscriber_identifier="imei")
    cb = get_dataframe(query)
    with pytest.raises(ValueError):
        cs = get_dataframe(query.counterparts_subset(include_subscribers=True))
