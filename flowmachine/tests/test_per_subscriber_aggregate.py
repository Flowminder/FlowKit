from flowmachine.features.subscriber.per_subscriber_aggregate import (
    PerSubscriberAggregate,
)
from flowmachine.features.subscriber.subscriber_call_durations import (
    PerLocationSubscriberCallDurations,
)


def test_per_subscriber_aggregate(get_dataframe):
    psa = PerSubscriberAggregate(
        subscriber_query=PerLocationSubscriberCallDurations("2016-01-01", "2016-01-02"),
        agg_column="value",
        agg_method="avg",
    )
    assert len(psa) >= 0
