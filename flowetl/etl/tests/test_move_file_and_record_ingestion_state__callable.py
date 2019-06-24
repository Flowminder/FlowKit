# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Testing the task callable that moves files from one location to another.
"""
from unittest.mock import patch, Mock
from pathlib import Path

import pendulum

from etl.production_task_callables import record_ingestion_state__callable

# pylint: disable=too-many-locals
def test_record_ingestion_state__callable(tmpdir, create_fake_dag_run):
    """
    Tests that we are able to issue the command to set the new state of the file
    within the ingestion process.
    """
    to_state = "archive"
    file_name = "file_to_move"
    cdr_type = "calls"
    cdr_date = pendulum.parse("2016-01-01").date()

    fake_dag_run = create_fake_dag_run(
        conf={"file_name": file_name, "cdr_type": cdr_type, "cdr_date": cdr_date}
    )

    mock_ETLRecord__set_state = Mock()

    # don't actually need a real session
    mock_get_session = Mock()
    mock_get_session.return_value = "session"

    assert mock_ETLRecord__set_state.mock_calls == []
    assert mock_get_session.mock_calls == []

    with patch(
        "etl.production_task_callables.ETLRecord.set_state", mock_ETLRecord__set_state
    ):
        with patch("etl.production_task_callables.get_session", mock_get_session):
            record_ingestion_state__callable(dag_run=fake_dag_run, to_state=to_state)

    assert len(mock_ETLRecord__set_state.mock_calls) == 1
    assert len(mock_get_session.mock_calls) == 1
    _, _, kwargs = mock_ETLRecord__set_state.mock_calls[0]

    assert kwargs == {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "state": to_state,
        "session": "session",
    }
