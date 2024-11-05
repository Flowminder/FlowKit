# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.topup_balance import *
from flowmachine.core import make_spatial_unit
from flowmachine.features.subscriber.daily_location import locate_subscribers

import pytest


@pytest.mark.parametrize(
    "statistic,msisdn,want",
    [
        ("count", "Rzx9WE1QRqdEX2Gp", 10),
        ("sum", "LBlWd64rqnMGv7kY", 44_575_009.52),
        ("avg", "JZoaw2jzvK2QMKYX", 224.419_246),
        ("max", "DELmRj9Vvl346G50", 529.73),
        ("min", "9vXy462Ej8V1kpWl", 442),
        ("median", "KXVqP6JyVDGzQa3b", 435.94),
        ("mode", "EkpjZe5z37W70QKA", 122.53),
        ("stddev", "EkpjZe5z37W70QKA", 15.095_273),
        ("variance", "JNK7mk5G1Dy6M2Ya", 505.767),
    ],
)
def test_topup_balance(get_dataframe, statistic, msisdn, want):
    """
    Test a few handpicked TopUpBalance instances.
    """
    query = TopUpBalance("2016-01-01", "2016-01-08", statistic=statistic)
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["statistic"])
def test_topup_balance_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in TopUpBalance."""

    with pytest.raises(ValueError):
        query = TopUpBalance("2016-01-03", "2016-01-05", **{kwarg: "error"})


def test_can_be_joined(get_dataframe):
    """
    TopUpBalance() can be joined with a location type metric.
    """
    topup_balance = TopUpBalance("2016-01-01", "2016-01-02", statistic="avg")
    dl = locate_subscribers(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
    )
    topup_balance_JA = topup_balance.join_aggregate(dl)
    df = get_dataframe(topup_balance_JA)
    assert topup_balance_JA.column_names == ["pcod", "value"]
