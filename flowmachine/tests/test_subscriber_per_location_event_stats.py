# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.per_location_event_stats import *

import pytest


@pytest.mark.parametrize(
    "statistic,msisdn,want,level",
    [
        ("count", "Rzx9WE1QRqdEX2Gp", 16, {}),
        ("sum", "LBlWd64rqnMGv7kY", 22, {}),
        ("avg", "JZoaw2jzvK2QMKYX", 1.333_333, {}),
        ("avg", "JZoaw2jzvK2QMKYX", 1.647_059, {"level": "admin3"}),
        ("avg", "JZoaw2jzvK2QMKYX", 1.285_714_2, {"direction": "in"}),
        ("max", "DELmRj9Vvl346G50", 4, {}),
        ("min", "9vXy462Ej8V1kpWl", 1, {}),
        ("stddev", "EkpjZe5z37W70QKA", 0.594_089, {}),
        ("variance", "JNK7mk5G1Dy6M2Ya", 0.395_833, {}),
    ],
)
def test_per_location_event_stats(get_dataframe, statistic, msisdn, want, level):
    """ Test hand-picked PerLocationEventStats. """
    query = PerLocationEventStats(
        "2016-01-01", "2016-01-06", statistic=statistic, **level
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


@pytest.mark.parametrize("kwarg", ["direction", "statistic"])
def test_per_location_event_stats_errors(kwarg):
    """ Test ValueError is raised for non-compliant kwarg in PerLocationEventStats. """

    with pytest.raises(ValueError):
        query = PerLocationEventStats("2016-01-03", "2016-01-05", **{kwarg: "error"})
