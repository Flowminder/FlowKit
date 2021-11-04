import pytest

from flowmachine.features.subscriber.unique_subscribers_from_queries import (
    UniqueSubscribersFromQueries,
)
from flowmachine.features.utilities.events_tables_union import EventTableSubset


def test_unique_subscribers_from_queries(get_dataframe):
    unique_subscriber_query = UniqueSubscribersFromQueries(
        [EventTableSubset(start="2016-01-01", stop="2016-01-03")]
    )
    # Need a better test
    assert len(get_dataframe(unique_subscriber_query)) > 0
