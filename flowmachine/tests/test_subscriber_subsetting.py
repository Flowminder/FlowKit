# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the subscriber subsetting functionality
"""


import pytest

from flowmachine.core import Table
from flowmachine.core.context import get_db
from flowmachine.features import (
    RadiusOfGyration,
    ModalLocation,
    UniqueSubscribers,
    daily_location,
    EventTableSubset,
)
from flowmachine.utils import list_of_dates


@pytest.mark.parametrize(
    "columns", [["msisdn"], None, ["id", "msisdn"]], ids=lambda x: f"{x}"
)
def test_events_table_subset_column_names(columns):
    """Test that EventTableSubset column_names property is accurate."""
    etu = EventTableSubset(
        start="2016-01-01", stop="2016-01-02", columns=columns, table="calls"
    )
    assert etu.head(0).columns.tolist() == etu.column_names


@pytest.mark.parametrize("ident", ("msisdn", "imei", "imsi"))
def test_events_table_subscriber_ident_substitutions(ident):
    """Test that EventTableSubset replaces the subscriber ident column name with subscriber."""
    etu = EventTableSubset(
        start="2016-01-01",
        stop="2016-01-02",
        columns=[ident],
        table="calls",
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
    engine = get_db().engine
    with engine.begin():
        sql = """CREATE TABLE subscriber_list (subscriber TEXT)"""
        engine.execute(sql)

        formatted_subscribers = ",".join("('{}')".format(u) for u in subscriber_list)
        sql = """INSERT INTO subscriber_list (subscriber) VALUES {}""".format(
            formatted_subscribers
        )
        engine.execute(sql)
    subs_table = Table("subscriber_list", columns=["subscriber"])
    subs_table.preflight()
    try:
        yield subs_table
    finally:
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
    su = ModalLocation(
        *[
            daily_location(d, subscriber_subset=subscriber_list)
            for d in list_of_dates("2016-01-01", "2016-01-07")
        ]
    )
    subsu = ModalLocation(
        *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-03")]
    ).subset("subscriber", subscriber_list)
    assert all(get_dataframe(su) == get_dataframe(subsu))


def test_query_can_be_subscriber_set_restricted(
    subscriber_list_table, subscriber_list, get_dataframe
):
    """Test that some queries can be limited to only a subset of subscribers."""

    rog = RadiusOfGyration(
        "2016-01-01", "2016-01-03", subscriber_subset=subscriber_list_table
    )
    hl = ModalLocation(
        *[
            daily_location(d, subscriber_subset=subscriber_list_table)
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    rog_df = get_dataframe(rog)
    hl_df = get_dataframe(hl)

    # Get the set of subscribers present in the dataframe, we need to handle the logic
    # of msisdn_from/msisdn_to
    calculated_subscriber_set = set(rog_df.subscriber)

    assert calculated_subscriber_set == set(subscriber_list)
    calculated_subscriber_set = set(hl_df.subscriber)

    assert calculated_subscriber_set == set(subscriber_list)


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
    unique_subs_sample = UniqueSubscribers("2016-01-01", "2016-01-07").random_sample(
        size=10, sampling_method="system", seed=0.1
    )
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
