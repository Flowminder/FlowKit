import pytest

from flowmachine.features.subscriber.unique_subscribers_from_queries import (
    UniqueSubscribersFromQueries,
)
from flowmachine.features.utilities.events_tables_union import EventTableSubset


def test_unique_subscribers_from_queries(get_dataframe):
    unique_subscriber_query = UniqueSubscribersFromQueries(
        [EventTableSubset(start="2016-01-01", stop="2016-01-03")]
    )
    # Using flowdb_synthetic_data (testdata seems to be empty?)
    print(get_dataframe(unique_subscriber_query))
    assert (
        get_dataframe(unique_subscriber_query).iloc[4]
        == "7e3229b9b15ca4cf00f7ed0b494b2378"
    )
