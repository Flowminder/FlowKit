# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import pendulum
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.exc import DataError
from sqlalchemy.orm import sessionmaker

from flowtrigger.model import Base, WorkflowRuns, init_db
from flowtrigger.utils import get_params_hash


def test_set_state(session):
    """
    Make sure we can add a row to the DB and the content
    we expect is in the DB afterwards.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state="in_process",
    )

    now = pendulum.parse("2016-01-02T13:00:01Z")
    with patch("pendulum.now", lambda x: now):
        WorkflowRuns.set_state(**workflow_run_data, session=session)

    rows = session.query(WorkflowRuns).all()
    assert len(rows) == 1

    row = rows[0]
    assert row.workflow_name == workflow_run_data["workflow_name"]
    assert row.workflow_params_hash == get_params_hash(
        workflow_run_data["workflow_params"]
    )
    assert row.reference_date == workflow_run_data["reference_date"]
    assert (
        pendulum.instance(row.scheduled_start_time)
        == workflow_run_data["scheduled_start_time"]
    )
    assert row.state.name == workflow_run_data["state"]
    assert pendulum.instance(row.timestamp) == now


def test_set_state_with_sqlite(sqlite_session):
    """
    Make sure we can add a row to a sqlite DB.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00"),
        state="in_process",
    )

    now = pendulum.parse("2016-01-02T13:00:01Z")
    with patch("pendulum.now", lambda x: now):
        WorkflowRuns.set_state(**workflow_run_data, session=sqlite_session)

    rows = sqlite_session.query(WorkflowRuns).all()
    assert len(rows) == 1

    row = rows[0]
    assert row.workflow_name == workflow_run_data["workflow_name"]
    assert row.workflow_params_hash == get_params_hash(
        workflow_run_data["workflow_params"]
    )
    assert row.reference_date == workflow_run_data["reference_date"]
    assert (
        pendulum.instance(row.scheduled_start_time)
        == workflow_run_data["scheduled_start_time"]
    )
    assert row.state.name == workflow_run_data["state"]
    assert pendulum.instance(row.timestamp) == now


def test_set_state_with_null_reference_date(session):
    """
    Test that we can add a row to the DB with reference_date=None.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=None,
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state="in_process",
    )

    WorkflowRuns.set_state(**workflow_run_data, session=session)

    row = session.query(WorkflowRuns).first()
    assert row.reference_date is None


def test_exception_raised_with_invalid_state(session):
    """
    Test that we get an exception raised when we try
    to add a new row with an invalid state.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
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
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
    )
    scheduled_start_time = pendulum.parse("2016-01-02T13:00:00Z")
    WorkflowRuns.set_state(
        **workflow_run_data,
        scheduled_start_time=scheduled_start_time,
        state="in_process",
        session=session,
    )
    WorkflowRuns.set_state(
        **workflow_run_data,
        scheduled_start_time=scheduled_start_time,
        state="failed",
        session=session,
    )

    state = WorkflowRuns.get_most_recent_state(**workflow_run_data, session=session)

    assert state.name == "failed"


@pytest.mark.parametrize(
    "state,expected", [("in_process", False), ("done", False), ("failed", True)]
)
def test_can_process(session, state, expected):
    """
    Test that can_process returns True for 'failed' runs, False for 'in_process' or 'done' runs.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
    )
    WorkflowRuns.set_state(
        **workflow_run_data,
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state=state,
        session=session,
    )

    res = WorkflowRuns.can_process(**workflow_run_data, session=session)
    assert res == expected


def test_can_process_new(session):
    """
    Test that 'can_process' returns True for a new workflow run.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
    )
    res = WorkflowRuns.can_process(**workflow_run_data, session=session)
    assert res


@pytest.mark.parametrize(
    "state,expected", [("in_process", False), ("done", True), ("failed", False)]
)
def test_is_done(session, state, expected):
    """
    Test that is_done returns True for 'done' runs, False otherwise.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
    )
    WorkflowRuns.set_state(
        **workflow_run_data,
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state=state,
        session=session,
    )

    res = WorkflowRuns.is_done(**workflow_run_data, session=session)
    assert res == expected


def test_new_workflow_run_is_not_done(session):
    """
    Test that 'is_done' returns False for a new workflow run that isn't yet in the DB.
    """
    workflow_run_data = dict(
        workflow_name="DUMMY_WORKFLOW_NAME",
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
    )
    res = WorkflowRuns.is_done(**workflow_run_data, session=session)
    assert not res


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
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state="in_process",
        session=session,
    )
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
        workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
        reference_date=pendulum.parse("2016-01-01", exact=True),
        scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
        state="in_process",
        session=session,
    )
    session.close()
    # Init DB
    init_db(postgres_test_db.url(), force=True)
    # Table should not contain data
    session = Session()
    assert len(session.query(WorkflowRuns).all()) == 0
    session.close()
