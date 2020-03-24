# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests the Random class which provides ways of selecting random
samples from the database.
"""
from typing import List

import pytest
import pickle

from flowmachine_core.query_bases.query import Query
from flowmachine_core.core.mixins import GraphMixin
from flowmachine_core.utility_queries.custom_query import CustomQuery
from flowmachine_core.query_bases.table import Table
from utility_queries.event_table_subset import EventTableSubset


def test_random_msisdn(get_dataframe):
    """
    Tests whether class selects a random sample of msisdn without failing.
    """
    df = get_dataframe(Table("events.calls", columns=["msisdn"]).random_sample(size=10))
    assert list(df.columns) == ["msisdn"]
    assert len(df) == 10


@pytest.mark.parametrize("sample_method", ["bernoulli", "system", "random_ids"])
def test_seeded_random(sample_method, get_dataframe):
    """
    Tests whether class selects a repeatable random sample.
    """

    df = get_dataframe(
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method=sample_method, seed=0.1
        )
    )
    df2 = get_dataframe(
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method=sample_method, seed=0.1
        )
    )
    assert df.values.tolist() == df2.values.tolist()


def test_bad_method_errors():
    """
    Bad sampling methods should raise an error.
    """

    with pytest.raises(ValueError):
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method="BAD_METHOD_TYPE", seed=-50
        )


def test_bad_must_provide_sample_size_or_fraction():
    """
    Should raise an error if neither sample size nor fraction is passed.
    """

    with pytest.raises(ValueError):
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=None, fraction=None
        )


def test_bad_must_provide_either_sample_size_or_fraction():
    """
    Should raise an error if both sample size and fraction are passed.
    """

    with pytest.raises(ValueError):
        Table("events.calls", columns=["msisdn"]).random_sample(size=10, fraction=0.5)


def test_seeded_random_oob():
    """
    Tests whether seeds are restricted to within +/-1.
    """

    with pytest.raises(ValueError):
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method="random_ids", seed=-50
        )


@pytest.mark.parametrize("sample_method", ["bernoulli", "system", "random_ids"])
def test_seeded_random_zero(sample_method):
    """
    Test that using 0 as seed results in reproducible outcomes
    """

    sample = Table("events.calls", columns=["msisdn"]).random_sample(
        size=10, sampling_method=sample_method, seed=0
    )
    assert sample.get_query() == sample.get_query()


def test_seeded_random_badmethod():
    """
    Tests whether seeds don't work with system_rows.
    """

    with pytest.raises(TypeError, match="got an unexpected keyword argument 'seed'"):
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method="system_rows", seed=-0.5
        )


def test_random_sites(get_dataframe):
    """
    Tests whether class selects a random sample of sites.
    """
    df = get_dataframe(
        Table("infrastructure.sites", columns=["id", "version"]).random_sample(size=5)
    )
    assert list(df.columns) == ["id", "version"]
    assert len(df) == 5


def test_random_from_query(get_dataframe):
    """
    Tests whether class selects random rows from query.
    """
    custom_query = CustomQuery(
        "SELECT id, version FROM infrastructure.sites", column_names=["id", "version"]
    )
    df = get_dataframe(custom_query.random_sample(size=7))
    assert len(df) == 7


def test_random_from_table(get_dataframe):
    """
    Tests whether class selects random rows from query.
    """
    df = get_dataframe(
        Table(name="infrastructure.sites", columns=["id", "version"]).random_sample(
            size=8
        )
    )
    assert list(df.columns) == ["id", "version"]
    assert len(df) == 8


def test_system_rows(get_dataframe):
    """
    Test whether the system_rows method runs without failing.
    """
    df = get_dataframe(
        Table("events.calls_20160101", columns=["msisdn"]).random_sample(
            size=10, sampling_method="system_rows"
        )
    )
    assert len(df) == 10
    df = get_dataframe(
        Table("events.calls_20160101", columns=["msisdn"]).random_sample(
            fraction=0.1, sampling_method="system_rows"
        )
    )
    assert len(df) == 125


def test_system(get_dataframe):
    """
    Test whether the system method runs without failing.
    """
    # it is necessary to run a while loop since sometimes the system method
    # does not return any rows.
    df = []
    while len(df) == 0:
        df = get_dataframe(
            Table("events.calls", columns=["msisdn"]).random_sample(
                size=20, sampling_method="system"
            )
        )
    assert list(df.columns) == ["msisdn"]
    assert len(df) == 20

    # it is necessary to run a while loop since sometimes the system method
    # does not return any rows.
    df = []
    while len(df) == 0:
        df = get_dataframe(
            Table("events.calls", columns=["msisdn"]).random_sample(
                fraction=0.25, sampling_method="system"
            )
        )
    assert list(df.columns) == ["msisdn"]


def test_bernoulli(get_dataframe):
    """
    Test whether the bernoulli method runs without failing.
    """
    df = get_dataframe(
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method="bernoulli"
        )
    )
    assert list(df.columns) == ["msisdn"]
    assert len(df) == 10

    df = get_dataframe(
        Table("events.calls", columns=["msisdn"]).random_sample(
            fraction=0.1, sampling_method="bernoulli"
        )
    )
    assert list(df.columns) == ["msisdn"]


def test_not_estimate_count(get_dataframe):
    """
    Test whether not estimating counts runs without failing.
    """
    df = get_dataframe(
        Table("events.calls", columns=["msisdn"]).random_sample(
            size=10, sampling_method="bernoulli", estimate_count=False
        )
    )
    assert list(df.columns) == ["msisdn"]
    assert len(df) == 10


def test_system_rows_fail_with_inheritance():
    """
    Test whether the system row method fails if the subscriber queries for random rows on a parent table.
    """
    with pytest.raises(ValueError):
        df = Table(name="events.calls").random_sample(
            size=8, sampling_method="system_rows"
        )


def test_random_sample(get_dataframe):
    """
    Test whether the random_sample method in the Query object works.
    """
    custom_query = CustomQuery(
        "SELECT id, version FROM infrastructure.sites", ["id", "version"]
    )
    df = get_dataframe(custom_query.random_sample(size=6))
    assert list(df.columns) == ["id", "version"]
    assert len(df) == 6


def test_is_subclass():
    """
    Test that a random sample is an instance of the sampled thing. 
    """
    qur = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sample = qur.random_sample(
        size=10, sampling_method="bernoulli", estimate_count=False
    )
    assert isinstance(sample, EventTableSubset)


def test_gets_parent_attributes():
    """
    Test that a random sample is an instance of the sampled thing.
    """

    class QueryWithExtraAttribute(Query):
        def __init__(self):
            self.extra = 1

        @property
        def column_names(self) -> List[str]:
            ["col"]

        def _make_query(self):
            return "SELECT 1 as col"

    qur = QueryWithExtraAttribute()
    sample = qur.random_sample(
        size=10, sampling_method="bernoulli", estimate_count=False
    )
    assert sample.extra == 1


def test_gets_mixins():
    """
    Test that a random sample gets applicable mixins. 
    """

    class QueryWithMixin(GraphMixin, Query):
        @property
        def column_names(self) -> List[str]:
            ["col"]

        def _make_query(self):
            return "SELECT 1 as col"

    assert isinstance(QueryWithMixin().random_sample(size=10), GraphMixin)


def test_pickling():
    """
    Test that we can pickle and unpickle random classes.
    """
    ss = Table("events.calls").random_sample(
        size=10, sampling_method="bernoulli", seed=0.73
    )
    assert ss.get_query() == pickle.loads(pickle.dumps(ss)).get_query()
    assert ss.query_id == pickle.loads(pickle.dumps(ss)).query_id
