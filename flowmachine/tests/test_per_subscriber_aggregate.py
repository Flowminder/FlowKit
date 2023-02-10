# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.subscriber.per_subscriber_aggregate import (
    PerSubscriberAggregate,
)
from flowmachine.features.subscriber.subscriber_call_durations import (
    PerLocationSubscriberCallDurations,
)
from flowmachine.core.dummy_query import DummyQuery

import pytest

from flowmachine.utils import Statistic


@pytest.fixture()
def per_location_query(flowmachine_connect):
    sq = PerLocationSubscriberCallDurations("2016-01-01", "2016-01-02")
    yield sq


class NonSubQuery(DummyQuery):
    @property
    def column_names(self):
        return ["value"]


@pytest.mark.parametrize("agg_method", Statistic)
def test_aggregates(get_dataframe, agg_method, per_location_query):
    psa = PerSubscriberAggregate(
        subscriber_query=per_location_query,
        agg_column="value",
        agg_method=agg_method,
    )
    assert (
        len(get_dataframe(psa))
        == get_dataframe(per_location_query).subscriber.nunique()
    )


def test_agg_method(get_dataframe, per_location_query):
    max_psa = PerSubscriberAggregate(
        subscriber_query=per_location_query, agg_column="value", agg_method="max"
    )
    min_psa = PerSubscriberAggregate(
        subscriber_query=per_location_query, agg_column="value", agg_method="min"
    )
    max_df = get_dataframe(max_psa)
    min_df = get_dataframe(min_psa)
    assert (max_df.value >= min_df.value).all()
    assert (max_df.value > min_df.value).any()


def test_agg_column_validation(per_location_query):
    with pytest.raises(ValueError, match="'nonexistant'"):
        psa = PerSubscriberAggregate(
            subscriber_query=per_location_query,
            agg_column="nonexistant",
            agg_method="avg",
        )


def test_agg_method_validation(per_location_query):
    with pytest.raises(ValueError, match="'bogosort'"):
        psa = PerSubscriberAggregate(
            subscriber_query=per_location_query,
            agg_column="value",
            agg_method="bogosort",
        )


def test_subscriber_column_validation():
    dq = NonSubQuery(dummy_param="foo")
    with pytest.raises(ValueError, match="'subscriber'"):
        psa = PerSubscriberAggregate(
            subscriber_query=dq, agg_column="value", agg_method="avg"
        )


def test_not_subscriber_validation(per_location_query):
    with pytest.raises(ValueError, match="'agg_column' cannot be 'subscriber'"):
        psa = PerSubscriberAggregate(
            subscriber_query=per_location_query,
            agg_column="subscriber",
            agg_method="avg",
        )
