# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.handset_stats import *

import pytest

from flowmachine.features import SubscriberHandsets


@pytest.mark.parametrize(
    "characteristic,statistic,msisdn,want",
    [
        ("width", "count", "lqOknAJRDNAewM10", 27),
        ("width", "sum", "LBlWd64rqnMGv7kY", 13941774.74),
        ("width", "avg", "JZoaw2jzvK2QMKYX", 28.0178288001543),
        ("width", "max", "DELmRj9Vvl346G50", 45.9),
        ("width", "min", "9vXy462Ej8V1kpWl", 5.19),
        ("width", "median", "KXVqP6JyVDGzQa3b", 17.1),
        ("width", "stddev", "EkpjZe5z37W70QKA", 7.08664242656798),
        ("width", "variance", "JNK7mk5G1Dy6M2Ya", 113.833539255934),
        ("height", "count", "JZoaw2jzvK2QMKYX", 34),
        ("depth", "count", "JZoaw2jzvK2QMKYX", 34),
        ("weight", "count", "JZoaw2jzvK2QMKYX", 34),
        ("display_width", "count", "JZoaw2jzvK2QMKYX", 34),
        ("display_height", "count", "JZoaw2jzvK2QMKYX", 34),
    ],
)
def test_handset_stats(get_dataframe, characteristic, statistic, msisdn, want):
    """
    Test a few handpicked HandsetStats instances.
    """
    query = HandsetStats(
        characteristic=characteristic,
        statistic=statistic,
        subscriber_handsets=SubscriberHandsets("2016-01-01", "2016-01-07"),
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["characteristic", "statistic"])
def test_handset_stats_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in HandsetStats."""

    if kwarg == "characteristic":
        kwargs = {"characteristic": "error"}
    else:
        kwargs = {"characteristic": "width", kwarg: "error"}

    with pytest.raises(ValueError):
        query = HandsetStats(
            subscriber_handsets=SubscriberHandsets("2016-01-03", "2016-01-05"), **kwargs
        )
