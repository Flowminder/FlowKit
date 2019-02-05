# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.mds_volume import *

import pytest


@pytest.mark.parametrize("statistic,msisdn,want", [
    ("count", "Rzx9WE1QRqdEX2Gp", 11),
    ("sum", "LBlWd64rqnMGv7kY", 11101.95),
    ("avg", "JZoaw2jzvK2QMKYX", 1210.263000),
    ("max", "DELmRj9Vvl346G50", 1509.11),
    ("min", "9vXy462Ej8V1kpWl", 137.38),
    ("median", "KXVqP6JyVDGzQa3b", 1245.540),
    ("stddev", "EkpjZe5z37W70QKA", 507.783502),
    ("variance", "JNK7mk5G1Dy6M2Ya", 91388.621845),
])
def test_mds_volume(get_dataframe, statistic, msisdn, want):
    """
    Test a few handpicked MDSVolume instances.
    """
    query = MDSVolume("2016-01-01", "2016-01-08", statistic=statistic)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc[msisdn, f"volume_{statistic}"] == pytest.approx(want)

def test_mds_volume_type(get_dataframe):
    """
    Test a few hand-picked MDSVolume instances with different volume types.
    """
    query = MDSVolume("2016-01-01", "2016-01-08", volume="upload")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["4oLKbnxm3vXqjMVx", "volume_sum"] == 7673.40

    query = MDSVolume("2016-01-01", "2016-01-08", volume="download")
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["YMBqRkzbbxGkX3zA", "volume_sum"] == 2568.49

def test_mds_wrong_volume_type_raises(get_dataframe):
    """
    Test that a wrong volume type raises error.
    """
    with pytest.raises(ValueError):
        query = MDSVolume("2016-01-01", "2016-01-08", volume="foo")
