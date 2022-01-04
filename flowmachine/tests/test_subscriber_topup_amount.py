# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.topup_amount import *

import pytest


@pytest.mark.parametrize(
    "statistic,msisdn,want",
    [
        ("count", "Rzx9WE1QRqdEX2Gp", 10),
        ("sum", "LBlWd64rqnMGv7kY", 45.71),
        ("avg", "JZoaw2jzvK2QMKYX", 4.556_667),
        ("max", "DELmRj9Vvl346G50", 9.16),
        ("min", "9vXy462Ej8V1kpWl", 1.64),
        ("median", "KXVqP6JyVDGzQa3b", 5.83),
        ("stddev", "EkpjZe5z37W70QKA", 1.759_553),
        ("variance", "JNK7mk5G1Dy6M2Ya", 4.70577),
    ],
)
def test_topup_amount(get_dataframe, statistic, msisdn, want):
    """
    Test a few handpicked TopUpAmount instances.
    """
    query = TopUpAmount("2016-01-01", "2016-01-08", statistic=statistic)
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["statistic"])
def test_topup_amount_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in TopUpAmount."""

    with pytest.raises(ValueError):
        query = TopUpAmount("2016-01-03", "2016-01-05", **{kwarg: "error"})
