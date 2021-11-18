from flowmachine.features.subscriber.per_subscriber_aggregate import (
    PerSubscriberAggregate,
    agg_methods,
)
from flowmachine.features.subscriber.subscriber_call_durations import (
    PerLocationSubscriberCallDurations,
)
from flowmachine.core.dummy_query import DummyQuery

import pytest


@pytest.fixture()
def per_location_query(flowmachine_connect):
    sq = PerLocationSubscriberCallDurations("2016-01-01", "2016-01-02")
    yield sq


class NonSubQuery(DummyQuery):
    @property
    def column_names(self):
        return ["value"]


@pytest.mark.parametrize("agg_method", agg_methods)
def test_aggregates(get_dataframe, agg_method, per_location_query):

    psa = PerSubscriberAggregate(
        subscriber_query=per_location_query,
        agg_column="value",
        agg_method=agg_method,
    )
    # per_location_query is 608 rows
    assert (
        len(get_dataframe(psa))
        == get_dataframe(per_location_query).subscriber.nunique()
    )


def test_agg_column_validation(per_location_query):
    with pytest.raises(ValueError):
        psa = PerSubscriberAggregate(
            subscriber_query=per_location_query,
            agg_column="nonexistant",
            agg_method="avg",
        )


def test_agg_method_validation(per_location_query):
    with pytest.raises(ValueError):
        psa = PerSubscriberAggregate(
            subscriber_query=per_location_query,
            agg_column="value",
            agg_method="bogosort",
        )


def test_subscriber_column_validation():
    dq = NonSubQuery(dummy_param="foo")
    with pytest.raises(ValueError):
        psa = PerSubscriberAggregate(
            subscriber_query=dq, agg_column="value", agg_method="avg"
        )
