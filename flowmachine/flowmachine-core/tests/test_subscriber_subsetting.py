# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the subscriber subsetting functionality
"""


import pytest

from flowmachine_core.query_bases.table import Table
from flowmachine_core.core.context import get_db
from flowmachine_core.utility_queries.event_table_subset import EventTableSubset
from utility_queries.custom_query import CustomQuery


@pytest.mark.parametrize(
    "columns", [["msisdn"], ["*"], ["id", "msisdn"]], ids=lambda x: f"{x}"
)
def test_events_table_subset_column_names(columns):
    """Test that EventTableSubset column_names property is accurate."""
    etu = EventTableSubset(
        start="2016-01-01", stop="2016-01-02", columns=columns, table="events.calls"
    )
    assert etu.head(0).columns.tolist() == etu.column_names


@pytest.mark.parametrize("ident", ("msisdn", "imei", "imsi"))
def test_events_table_subscriber_ident_substitutions(ident):
    """Test that EventTableSubset replaces the subscriber ident column name with subscriber."""
    etu = EventTableSubset(
        start="2016-01-01",
        stop="2016-01-02",
        columns=[ident],
        table="events.calls",
        subscriber_identifier=ident,
    )
    assert "subscriber" == etu.head(0).columns[0]
    assert ["subscriber"] == etu.column_names


@pytest.fixture
def subscriber_list():
    return [
        "a7wRAnjb2znjOlxK",
        "Q7lRVD4YY1xawymG",
        "5Kgwy8Gp6DlN3Eq9",
        "BKMy1nYEZpnoEA7G",
        "NQV3J52PeYgbLm2w",
        "pD0me6Do0dNE1nYZ",
        "KyonmeOzDkEN40qP",
        "vBR2gwl2B0nDJob5",
        "AwQr1M3w8mMvV3p4",
    ]


@pytest.fixture
def subscriber_list_table(subscriber_list, flowmachine_connect):
    formatted_subscribers = ",".join("('{}')".format(u) for u in subscriber_list)
    subs_table = (
        CustomQuery(
            f"SELECT * FROM (VALUES {formatted_subscribers}) as t(subscriber)",
            column_names=["subscriber"],
        )
        .store()
        .result()
        .get_table()
    )
    yield subs_table
    subs_table.invalidate_db_cache(drop=True)


def test_cdrs_can_be_subset_by_table(
    subscriber_list_table, get_dataframe, subscriber_list
):
    """
    We can subset CDRs by a table in the database.
    """

    su = EventTableSubset(
        start="2016-01-01", stop="2016-01-03", subscriber_subset=subscriber_list_table
    )

    df = get_dataframe(su)

    # Get the set of subscribers present in the dataframe, we need to handle the logic
    # of msisdn_from/msisdn_to
    calculated_subscriber_set = set(df.subscriber)

    assert calculated_subscriber_set == set(subscriber_list)


def test_subset_correct(subscriber_list, get_dataframe):
    """Test that pushed in subsetting matches .subset result"""
    su = EventTableSubset(
        start="2016-01-01", stop="2016-01-03", subscriber_subset=subscriber_list
    )
    subsu = EventTableSubset(start="2016-01-01", stop="2016-01-03").subset(
        "subscriber", subscriber_list
    )
    assert all(get_dataframe(su) == get_dataframe(subsu))


def test_cdrs_can_be_subset_by_list(get_dataframe, subscriber_list):
    """
    We can subset CDRs with a list.
    """

    su = EventTableSubset(
        start="2016-01-01", stop="2016-01-03", subscriber_subset=subscriber_list
    )
    df = get_dataframe(su)

    # Get the set of subscribers present in the dataframe, we need to handle the logic
    # of msisdn_from/msisdn_to
    calculated_subscriber_set = set(df.subscriber)

    assert calculated_subscriber_set == set(subscriber_list)


def test_can_subset_by_sampler(get_dataframe):
    """Test that we can use the output of another query to subset by."""
    unique_subs_sample = CustomQuery(
        "SELECT msisdn as subscriber FROM events.calls", column_names=["subscriber"]
    ).random_sample(size=10, sampling_method="system", seed=0.1)
    su = EventTableSubset(
        start="2016-01-01", stop="2016-01-03", subscriber_subset=unique_subs_sample
    )
    su_set = set(get_dataframe(su).subscriber)
    uu_set = set(get_dataframe(unique_subs_sample).subscriber)
    assert su_set == uu_set
    assert len(su_set) == 10


def test_omitted_subscriber_column(get_dataframe, subscriber_list):
    """Test that a result is returned and warning is raised when omitting a subscriber column."""
    with pytest.warns(UserWarning):
        su_omit_col = get_dataframe(
            EventTableSubset(
                start="2016-01-01",
                stop="2016-01-03",
                subscriber_subset=subscriber_list,
                columns=["duration"],
            )
        )
    su_all_cols = get_dataframe(
        EventTableSubset(
            start="2016-01-01",
            stop="2016-01-03",
            subscriber_subset=subscriber_list,
            columns=["msisdn", "duration"],
        )
    )
    assert su_omit_col.duration.values.tolist() == su_all_cols.duration.values.tolist()
    assert su_omit_col.columns.tolist() == ["duration"]
