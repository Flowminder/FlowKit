# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features.subscriber.per_location_event_stats import *

import pytest


@pytest.mark.parametrize(
    "kwargs,msisdn,want",
    [
        ({"statistic": "count"}, "Rzx9WE1QRqdEX2Gp", 16),
        ({"statistic": "sum"}, "LBlWd64rqnMGv7kY", 22),
        ({"statistic": "avg"}, "JZoaw2jzvK2QMKYX", 1.333_333),
        (
            {
                "statistic": "avg",
                "spatial_unit": {"spatial_unit_type": "admin", "level": 3},
            },
            "JZoaw2jzvK2QMKYX",
            1.647_059,
        ),
        ({"statistic": "avg", "direction": "in"}, "JZoaw2jzvK2QMKYX", 1.285_714_2),
        ({"statistic": "max"}, "DELmRj9Vvl346G50", 4),
        ({"statistic": "min"}, "9vXy462Ej8V1kpWl", 1),
        ({"statistic": "stddev"}, "EkpjZe5z37W70QKA", 0.594_089),
        ({"statistic": "variance"}, "JNK7mk5G1Dy6M2Ya", 0.395_833),
    ],
)
def test_per_location_event_stats(get_dataframe, kwargs, msisdn, want):
    """Test hand-picked PerLocationEventStats."""
    if "spatial_unit" in kwargs:
        kwargs["spatial_unit"] = make_spatial_unit(**kwargs["spatial_unit"])
    query = PerLocationEventStats("2016-01-01", "2016-01-06", **kwargs)
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["direction", "statistic"])
def test_per_location_event_stats_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in PerLocationEventStats."""

    with pytest.raises(ValueError):
        query = PerLocationEventStats("2016-01-03", "2016-01-05", **{kwarg: "error"})
