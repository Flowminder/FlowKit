# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import pandas as pd
import pytest

import flowmachine
from flowmachine.core import CustomQuery
from flowmachine.core.subscriber_subsetter import *

# TODO: This call to flowmachine.connect() is needed to create
flowmachine.connect()


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
                "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 200"
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
    flowmachine_query = CustomQuery("SELECT msisdn, duration FROM events.calls")
    with pytest.raises(ValueError):
        _ = make_subscriber_subsetter(flowmachine_query)
