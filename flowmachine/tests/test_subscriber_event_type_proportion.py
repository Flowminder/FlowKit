# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.event_type_proportion import *

import pytest


@pytest.mark.parametrize(
    "numerator, msisdn, want",
    [
        ("events.calls", "AgB6KR3Levd9Z1vJ", 0.351_852),
        ("events.sms", "7ra3xZakjEqB1Al5", 0.362_069),
        ("events.mds", "QrAlXqDbXDkNJe3E", 0.236_363_63),
        ("events.topups", "bKZLwjrMQG7z468y", 0.183_098_5),
        (["events.calls", "events.sms"], "AgB6KR3Levd9Z1vJ", 0.6481481),
    ],
)
def test_proportion_event_type(get_dataframe, numerator, msisdn, want):
    """
    Test some hand picked periods and tables
    """
    query = ProportionEventType(
        "2016-01-01",
        "2016-01-08",
        numerator,
        tables=[
            "events.calls",
            "events.sms",
            "events.mds",
            "events.topups",
            "events.forwards",
        ],
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)

    query = ProportionEventType(
        "2016-01-02", "2016-01-04", numerator, tables=numerator
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value.unique() == [1]
