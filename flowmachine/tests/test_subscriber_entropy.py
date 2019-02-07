# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.entropy import *
from flowmachine.core import CustomQuery
from flowmachine.core.errors.flowmachine_errors import MissingDirectionColumnError

import numpy as np
import pytest


class MockEntropy(BaseEntropy):
    def __init__(self):

        subscribers = np.random.choice(["a", "b", "c", "d", "e"], 20)
        frequencies = np.random.randint(1, 20, 20)

        self.entropy = {}

        for i in np.unique(subscribers):
            abs = frequencies[subscribers == i]
            rel = abs / abs.sum()
            self.entropy[i] = (-1 * rel * np.log(rel)).sum()

        self.vals = ", ".join(
            [f"('{s}', {f})" for s, f in zip(subscribers, frequencies)]
        )

    @property
    def _absolute_freq_query(self):

        return f"""
        SELECT * FROM (VALUES {self.vals}) AS u (subscriber, absolute_freq)
        """


def test_base_entropy(get_dataframe):
    """
    Test that the base class calculates the correct entropy for a random list of frequencies.
    """
    query = MockEntropy()
    df = get_dataframe(query).set_index("subscriber")
    assert df.entropy.to_dict() == pytest.approx(query.entropy)


def test_subscriber_periodic_entropy(get_dataframe):
    """
    Test some hand picked periods and tables.
    """
    query = PeriodicEntropy("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["09NrjaNNvDanD8pk"].entropy == pytest.approx(2.906_541)

    query = PeriodicEntropy("2016-01-01", "2016-01-07", direction="in")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0Gl95NRLjW2aw8pW"].entropy == pytest.approx(2.271_869)

    query = PeriodicEntropy("2016-01-02", "2016-01-08", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0DB8zw67E9mZAPK2"].entropy == pytest.approx(2.304_619)


def test_subscriber_location_entropy(get_dataframe):
    """
    Test some hand picked periods and tables.
    """
    query = LocationEntropy("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0DB8zw67E9mZAPK2"].entropy == pytest.approx(2.996_587)

    query = LocationEntropy("2016-01-02", "2016-01-05", level="admin1")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0DB8zw67E9mZAPK2"].entropy == pytest.approx(1.214_889_6)


def test_subscriber_contact_entropy(get_dataframe):
    """
    Test some hand picked periods and tables.
    """
    query = ContactEntropy("2016-01-01", "2016-01-08")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["gPZ7jbqlnAXR3JG5"].entropy == pytest.approx(0.673_012)

    query = ContactEntropy("2016-01-01", "2016-01-08", direction="out")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0DB8zw67E9mZAPK2"].entropy == 0
    assert df.loc["VkzMxYjv7mYn53oK"].entropy == pytest.approx(0.679_838)

    query = ContactEntropy("2016-01-01", "2016-01-08", direction="in")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["0Gl95NRLjW2aw8pW"].entropy == 0
    assert df.loc["VkzMxYjv7mYn53oK"].entropy == pytest.approx(0.680_629)
