# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.per_contact_event_stats import *
from flowmachine.features import ContactBalance

import pytest


@pytest.mark.parametrize(
    "statistic,msisdn,want",
    [
        ("count", "Rzx9WE1QRqdEX2Gp", 2),
        ("sum", "LBlWd64rqnMGv7kY", 16),
        ("avg", "JZoaw2jzvK2QMKYX", 12.5),
        ("max", "DELmRj9Vvl346G50", 14),
        ("min", "9vXy462Ej8V1kpWl", 4),
        ("median", "KXVqP6JyVDGzQa3b", 8),
        ("stddev", "EkpjZe5z37W70QKA", 0),
        ("variance", "JNK7mk5G1Dy6M2Ya", 2),
    ],
)
def test_per_contact_event_stats(get_dataframe, statistic, msisdn, want):
    """Test hand-picked PerContactEventStats."""
    query = PerContactEventStats(ContactBalance("2016-01-02", "2016-01-06"), statistic)
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["statistic"])
def test_per_contact_event_stats_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in PerContactEventStats."""

    with pytest.raises(ValueError):
        query = PerContactEventStats(
            ContactBalance("2016-01-02", "2016-01-06"), **{kwarg: "error"}
        )
