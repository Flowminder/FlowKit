# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features.utilities.events_tables_union import EventsTablesUnion


@pytest.mark.parametrize(
    "columns", [["msisdn"], ["*"], ["id", "msisdn"]], ids=lambda x: f"{x}"
)
def test_events_tables_union_column_names(columns):
    """Test that EventsTableUnion column_names property is accurate."""
    etu = EventsTablesUnion(
        "2016-01-01", "2016-01-02", columns=columns, tables=["events.calls"]
    )
    assert etu.head(0).columns.tolist() == etu.column_names


@pytest.mark.parametrize("ident", ("msisdn", "imei", "imsi"))
def test_events_table_union_subscriber_ident_substitutions(ident):
    """Test that EventTableSubset replaces the subscriber ident column name with subscriber."""
    etu = EventsTablesUnion(
        "2016-01-01",
        "2016-01-02",
        columns=[ident],
        tables=["events.calls"],
        subscriber_identifier=ident,
    )
    assert "subscriber" == etu.head(0).columns[0]
    assert ["subscriber"] == etu.column_names


def test_events_tables_union_raises_error():
    """EventsTablesUnion should error when trying to use all columns with disparate event types"""
    with pytest.raises(ValueError):
        EventsTablesUnion("2016-01-01", "2016-01-02", columns=["*"])


def test_length(get_length):
    """
    Test that EventsTablesUnion has the correct length
    """
    etu = EventsTablesUnion(
        "2016-01-01", "2016-01-02", columns=["msisdn", "msisdn_counterpart", "datetime"]
    )
    assert get_length(etu) == 2500


def test_get_only_sms(get_length):
    """
    Test that we can get only sms
    """

    etu = EventsTablesUnion(
        "2016-01-01",
        "2016-01-02",
        columns=["msisdn", "msisdn_counterpart", "datetime"],
        tables="events.sms",
    )
    assert get_length(etu) == 1246


@pytest.mark.parametrize(
    "arg,error_type,error_message",
    [
        ("", ValueError, "Empty table name."),
        (0, ValueError, "Tables must be a string or list of strings."),
        ([0, "a"], ValueError, "Tables must be a string or list of strings."),
        ([], ValueError, "Empty tables list."),
    ],
)
def test_bad_table_arguments(arg, error_type, error_message):
    """
    Test that an appropriate error is raised for bad tables arguments.
    """

    with pytest.raises(expected_exception=error_type, match=error_message):
        EventsTablesUnion(
            "2016-01-01",
            "2016-01-02",
            columns=["msisdn"],
            tables=arg,
        )


def test_get_list_of_tables(get_length):
    """
    Test that we can get only sms
    """

    etu = EventsTablesUnion(
        "2016-01-01",
        "2016-01-02",
        columns=["msisdn", "msisdn_counterpart", "datetime"],
        tables=["events.calls", "events.sms"],
    )
    assert get_length(etu) == 2500
