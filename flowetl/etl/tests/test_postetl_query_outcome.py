# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for the postetl book-keeping DB Model
"""
from unittest.mock import patch

import pendulum
import pytest

from etl.model import ETLPostQueryOutcome


def test_can_set_outcome(session):
    """
    Make sure we can add a row to the DB and the content
    we expect is in the DB afterwards.
    """
    query_data = {
        "cdr_type": "calls",
        "cdr_date": pendulum.parse("2016-01-01").date(),
        "type_of_query_or_check": "query_type",
        "outcome": "outcome",
        "optional_comment_or_description": "Optional description",
    }

    now = pendulum.parse("2016-01-01")
    with patch("pendulum.utcnow", lambda: now):
        ETLPostQueryOutcome.set_outcome(
            cdr_type=query_data["cdr_type"],
            cdr_date=query_data["cdr_date"],
            type_of_query_or_check=query_data["type_of_query_or_check"],
            outcome=query_data["outcome"],
            optional_comment_or_description=query_data[
                "optional_comment_or_description"
            ],
            session=session,
        )

    rows = session.query(ETLPostQueryOutcome).all()
    assert len(rows) == 1

    row = rows[0]
    assert row.cdr_type == query_data["cdr_type"]
    assert row.cdr_date == query_data["cdr_date"]
    assert row.type_of_query_or_check == query_data["type_of_query_or_check"]
    assert row.outcome == query_data["outcome"]
    assert row.timestamp == now
    assert (
        row.optional_comment_or_description
        == query_data["optional_comment_or_description"]
    )


def test_exception_raised_with_invalid_cdr_type(session):
    """
    Test that we get an exception raised when we try
    to add a new row with an invalid cdr_type.
    """
    query_data = {
        "cdr_type": "spaghetti",
        "cdr_date": pendulum.parse("2016-01-01").date(),
        "type_of_query_or_check": "query_type",
        "outcome": "outcome",
        "optional_comment_or_description": "Optional description",
    }
    with pytest.raises(ValueError):
        ETLPostQueryOutcome.set_outcome(
            cdr_type=query_data["cdr_type"],
            cdr_date=query_data["cdr_date"],
            type_of_query_or_check=query_data["type_of_query_or_check"],
            outcome=query_data["outcome"],
            optional_comment_or_description=query_data[
                "optional_comment_or_description"
            ],
            session=session,
        )
