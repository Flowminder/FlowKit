# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Testing the postload task callable that runs informative queries.
"""
from unittest.mock import patch, Mock

import pendulum

from etl.production_task_callables import run_postload_queries__callable

# pylint: disable=too-many-locals
def test_run_postload_queries__callable(create_fake_dag_run):
    """
    Tests that we are able to run and save the outcome of informative
    queries within the ingestion process.
    """
    file_name = "test_file"
    cdr_type = "calls"
    cdr_date = pendulum.parse("2016-01-01").date()

    def mock_postetl_query_outcome(**kwargs):
        return lambda cdr_type, cdr_date, session: kwargs

    queries = {
        cdr_type: [
            mock_postetl_query_outcome(
                outcome="11",
                type_of_query_or_check="12",
                optional_comment_or_description="13",
            ),
            mock_postetl_query_outcome(outcome="21", type_of_query_or_check="22"),
        ]
    }

    fake_dag_run = create_fake_dag_run(
        conf={"file_name": file_name, "cdr_type": cdr_type, "cdr_date": cdr_date}
    )

    mock_ETLPostQueryOutcome__set_outcome = Mock()

    # don't actually need a real session
    mock_get_session = Mock()
    mock_get_session.return_value = "session"

    assert mock_ETLPostQueryOutcome__set_outcome.mock_calls == []
    assert mock_get_session.mock_calls == []

    with patch(
        "etl.production_task_callables.ETLPostQueryOutcome.set_outcome",
        mock_ETLPostQueryOutcome__set_outcome,
    ):
        with patch("etl.production_task_callables.get_session", mock_get_session):
            run_postload_queries__callable(dag_run=fake_dag_run, queries=queries)

    assert len(mock_ETLPostQueryOutcome__set_outcome.mock_calls) == 2
    assert len(mock_get_session.mock_calls) == 1
    _, _, kwargs1 = mock_ETLPostQueryOutcome__set_outcome.mock_calls[0]
    _, _, kwargs2 = mock_ETLPostQueryOutcome__set_outcome.mock_calls[1]

    assert kwargs1 == {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "outcome": "11",
        "type_of_query_or_check": "12",
        "optional_comment_or_description": "13",
        "session": "session",
    }

    assert kwargs2 == {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "outcome": "21",
        "type_of_query_or_check": "22",
        "optional_comment_or_description": "",
        "session": "session",
    }
