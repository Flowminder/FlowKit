# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.handset_stats import *

import pytest

from flowmachine.features import SubscriberHandsets

# the test db contains no handset information so we can only test if the query
# runs without raising errors, the calculation is very similar to
# topup_balances which gives some assurance that they are correct
@pytest.mark.parametrize(
    "characteristic,statistic,msisdn,want",
    [
        ("width", "count", "lqOknAJRDNAewM10", 0),
        ("width", "sum", "LBlWd64rqnMGv7kY", None),
        ("width", "avg", "JZoaw2jzvK2QMKYX", None),
        ("width", "max", "DELmRj9Vvl346G50", None),
        ("width", "min", "9vXy462Ej8V1kpWl", None),
        ("width", "median", "KXVqP6JyVDGzQa3b", None),
        ("width", "stddev", "EkpjZe5z37W70QKA", None),
        ("width", "variance", "JNK7mk5G1Dy6M2Ya", None),
        ("height", "count", "JZoaw2jzvK2QMKYX", 0),
        ("depth", "count", "JZoaw2jzvK2QMKYX", 0),
        ("weight", "count", "JZoaw2jzvK2QMKYX", 0),
        ("display_width", "count", "JZoaw2jzvK2QMKYX", 0),
        ("display_height", "count", "JZoaw2jzvK2QMKYX", 0),
    ],
)
def test_handset_stats(get_dataframe, characteristic, statistic, msisdn, want):
    """
    Test a few handpicked HandsetStats instances.
    """
    query = HandsetStats(
        "2016-01-01", "2016-01-07", characteristic=characteristic, statistic=statistic
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == want


@pytest.mark.parametrize("kwarg", ["characteristic", "statistic"])
def test_handset_stats_errors(kwarg):
    """ Test ValueError is raised for non-compliant kwarg in HandsetStats. """

    if kwarg == "characteristic":
        kwargs = {"characteristic": "error"}
    else:
        kwargs = {"characteristic": "width", kwarg: "error"}

    with pytest.raises(ValueError):
        query = HandsetStats("2016-01-03", "2016-01-05", **kwargs)
