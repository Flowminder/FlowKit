# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.event_type_proportion import *

import pytest


@pytest.mark.parametrize(
    "numerator, numerator_direction, msisdn, want",
    [
        ("calls", "both", "AgB6KR3Levd9Z1vJ", 0.351_852),
        ("calls", "in", "AgB6KR3Levd9Z1vJ", 0.203_703_703_7),
        ("sms", "both", "7ra3xZakjEqB1Al5", 0.362_069),
        ("mds", "both", "QrAlXqDbXDkNJe3E", 0.236_363_63),
        ("topups", "both", "bKZLwjrMQG7z468y", 0.183_098_5),
        (["calls", "sms"], "both", "AgB6KR3Levd9Z1vJ", 0.648_148_1),
    ],
)
def test_proportion_event_type(
    get_dataframe, numerator, numerator_direction, msisdn, want
):
    """
    Test some hand picked periods and tables
    """
    query = ProportionEventType(
        "2016-01-01",
        "2016-01-08",
        numerator,
        numerator_direction=numerator_direction,
        tables=[
            "calls",
            "sms",
            "mds",
            "topups",
            "forwards",
        ],
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)

    query = ProportionEventType(
        "2016-01-02",
        "2016-01-04",
        numerator,
        numerator_direction=numerator_direction,
        tables=numerator,
        direction=numerator_direction,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value.unique() == [1]
