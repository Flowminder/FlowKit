# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from unittest.mock import patch

import pendulum
from sqlalchemy import create_engine
from sqlalchemy.exc import DataError
from sqlalchemy.orm import sessionmaker

from autoflow.model import Base, WorkflowRuns, RunState, init_db
from autoflow.utils import get_params_hash


def test_set_state(session):
    """
    Make sure we can add a row to the DB and the content
    we expect is in the DB afterwards.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        state=RunState.running,
    )

    now = pendulum.parse("2016-01-02T13:00:01Z")
    with patch("pendulum.now", lambda x: now):
        WorkflowRuns.set_state(**workflow_run_data, session=session)

    rows = session.query(WorkflowRuns).all()
    assert len(rows) == 1

    row = rows[0]
    assert row.workflow_name == workflow_run_data["workflow_name"]
    assert row.parameters_hash == get_params_hash(workflow_run_data["parameters"])
    assert row.state == workflow_run_data["state"]
    assert pendulum.instance(row.timestamp) == now


def test_set_state_with_sqlite(sqlite_session):
    """
    Make sure we can add a row to a sqlite DB.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        state=RunState.running,
    )

    now = pendulum.parse("2016-01-02T13:00:01Z")
    with patch("pendulum.now", lambda x: now):
        WorkflowRuns.set_state(**workflow_run_data, session=sqlite_session)

    rows = sqlite_session.query(WorkflowRuns).all()
    assert len(rows) == 1

    row = rows[0]
    assert row.workflow_name == workflow_run_data["workflow_name"]
    assert row.parameters_hash == get_params_hash(workflow_run_data["parameters"])
    assert row.state == workflow_run_data["state"]
    assert pendulum.instance(row.timestamp) == now


def test_exception_raised_with_invalid_state(session):
    """
    Test that we get an exception raised when we try
    to add a new row with an invalid state.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        state="INVALID_STATE",
    )
    with pytest.raises(DataError):
        WorkflowRuns.set_state(**workflow_run_data, session=session)


def test_get_most_recent_state(session):
    """
    Test that get_most_recent_state returns the most recent state.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
    )
    WorkflowRuns.set_state(**workflow_run_data, state=RunState.running, session=session)
    WorkflowRuns.set_state(**workflow_run_data, state=RunState.failed, session=session)

    state = WorkflowRuns.get_most_recent_state(**workflow_run_data, session=session)

    assert state == RunState.failed


def test_get_most_recent_state_returns_None(session):
    """
    Test that get_most_recent_state returns None if a workflow run has no previous state.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
    )

    state = WorkflowRuns.get_most_recent_state(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        session=session,
    )

    assert state is None


def test_init_db_doesnt_wipe(postgres_test_db):
    """
    DB shouldn't get reinitialised if already built.
    """
    # Write a row to the workflow_runs table
    engine = create_engine(postgres_test_db.url())
    Session = sessionmaker(bind=engine)
    session = Session()
    WorkflowRuns.set_state(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        state=RunState.running,
        session=session,
    )
    session.commit()
    session.close()
    # Init DB
    init_db(postgres_test_db.url())
    # Table should still contain data
    session = Session()
    assert len(session.query(WorkflowRuns).all()) > 0
    session.close()


def test_init_db_force(postgres_test_db):
    """
    DB should be wiped clean if force is true.
    """
    # Write a row to the workflow_runs table
    engine = create_engine(postgres_test_db.url())
    Session = sessionmaker(bind=engine)
    session = Session()
    WorkflowRuns.set_state(
        workflow_name="DUMMY_WORKFLOW_NAME",
        parameters={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        state=RunState.running,
        session=session,
    )
    session.commit()
    session.close()
    # Init DB
    init_db(postgres_test_db.url(), force=True)
    # Table should not contain data
    session = Session()
    assert len(session.query(WorkflowRuns).all()) == 0
    session.close()
