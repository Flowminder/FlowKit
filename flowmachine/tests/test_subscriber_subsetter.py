# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import numpy as np
import pandas as pd
import pytest

import flowmachine
from flowmachine.core import CustomQuery
from flowmachine.core.subscriber_subsetter import *
from flowmachine.features import daily_location

flowmachine.connect()
# TODO: This is necessary because it is not possible to use a Query object without a connection,
# and no connection is available when the parameterised tests are collected


@pytest.mark.parametrize(
    "input_subset, expected_subsetter_type",
    [
        ("all", SubscriberSubsetterForAllSubscribers),
        (None, SubscriberSubsetterForAllSubscribers),
        ("<SINGLE_SUBSCRIBER_ID>", SubscriberSubsetterForExplicitSubset),
        (
            ["<SUBSCRIBER_ID_1>", "<SUBSCRIBER_ID_2>", "<SUBSCRIBER_ID_3>"],
            SubscriberSubsetterForExplicitSubset,
        ),
        (
            ("<SUBSCRIBER_ID_1>", "<SUBSCRIBER_ID_2>"),
            SubscriberSubsetterForExplicitSubset,
        ),
        (
            np.array(["<SUBSCRIBER_ID_1>", "<SUBSCRIBER_ID_2>"]),
            SubscriberSubsetterForExplicitSubset,
        ),
        (
            pd.Series(["<SUBSCRIBER_ID_1>", "<SUBSCRIBER_ID_2>"]),
            SubscriberSubsetterForExplicitSubset,
        ),
        (
            CustomQuery(
                "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 200",
                ["duration", "subscriber"],
            ),
            SubscriberSubsetterForFlowmachineQuery,
        ),
    ],
)
def test_can_create_subscriber_subsetter_from_different_input_types(
    input_subset, expected_subsetter_type
):
    """
    The factory function make_subscriber_subsetter() accepts supported input types and returns an appropriate instance of SubscriberSubsetterBase.
    """
    subsetter = make_subscriber_subsetter(input_subset)
    assert isinstance(subsetter, expected_subsetter_type)


def test_raises_error_if_flowmachine_query_does_not_contain_subscriber_column():
    """
    An error is raised when creating a subsetter from a flowmachine query that doesn't contain a column named 'subscriber'.
    """
    flowmachine_query = CustomQuery(
        "SELECT msisdn, duration FROM events.calls", ["msisdn", "duration"]
    )
    with pytest.raises(ValueError):
        _ = make_subscriber_subsetter(flowmachine_query)


def test_subsetting_of_query(get_dataframe):
    """
    Check that query ids and length of results of some subsetted queries are as expected.
    """
    selected_subscriber_ids = [
        "1jwYL3Nl1Y46lNeQ",
        "nLvm2gVnEdg7lzqX",
        "jwKJorl0yBrZX5N8",
    ]
    custom_query = CustomQuery(
        "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 10",
        ["duration", "subscriber"],
    )
    subsetter_1 = SubscriberSubsetterForAllSubscribers()
    subsetter_2 = SubscriberSubsetterForExplicitSubset(selected_subscriber_ids)
    subsetter_3 = SubscriberSubsetterForFlowmachineQuery(custom_query)

    dl_0 = daily_location(date="2016-01-01")
    dl_1 = daily_location(date="2016-01-01", subscriber_subset=subsetter_1)
    dl_2 = daily_location(date="2016-01-01", subscriber_subset=subsetter_2)
    dl_3 = daily_location(date="2016-01-01", subscriber_subset=subsetter_3)

    assert 499 == len(get_dataframe(dl_0))
    assert 499 == len(get_dataframe(dl_1))
    assert 3 == len(get_dataframe(dl_2))
    assert 26 == len(get_dataframe(dl_3))
