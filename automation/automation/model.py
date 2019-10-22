# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import enum
import pendulum
import json
from typing import Dict, Any, Optional, Union

from sqlalchemy import (
    Column,
    String,
    DateTime,
    Date,
    Integer,
    Enum,
    JSON,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base

from .utils import get_params_hash

Base = declarative_base()


class RunState(enum.Enum):
    """
    possible states a flow run can be in.
    """

    in_process = 1
    done = 2
    failed = 3


class workflow_runs(Base):
    """
    DB Model for storing the state of flow runs.
    """

    __tablename__ = "workflow_runs"
    # __table_args__ = {"schema": "automation"}

    id = Column(Integer, primary_key=True)
    workflow_name = Column(String)
    workflow_params_hash = Column(String)
    reference_date = Column(Date)
    scheduled_start_time = Column(DateTime(timezone=True))
    state = Column(Enum(RunState))
    timestamp = Column(DateTime(timezone=True))

    def __init__(
        self,
        workflow_name: str,
        workflow_params_hash: str,
        reference_date: Union["datetime.date", None],
        scheduled_start_time: "datetime.datetime",
        state: str,
    ):
        self.workflow_name = workflow_name
        self.workflow_params_hash = (
            workflow_params_hash
        )  # Hash generated from workflow parameters dict
        self.reference_date = reference_date
        self.scheduled_start_time = (
            scheduled_start_time
        )  # Time at which the workflow run started
        self.state = state  # Flow run state
        self.timestamp = pendulum.now("utc")

    @classmethod
    def set_state(
        cls,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        reference_date: Union["datetime.date", None],
        scheduled_start_time: "datetime.datetime",
        state: str,
        session: "sqlalchemy.orm.session.Session",
    ) -> None:
        """
        Add a new row to the workflow runs table.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        workflow_params : dict
            Parameters passed when running the workflow
        reference_date : date or None
            The date with which the workflow run is associated
        scheduled_start_time : datetime
            Scheduled start time of the workflow run
        state : str
            The state of the workflow run ("in_process", "done" or "failed")
        session : Session
            A sqlalchemy session for a DB in which this model exists.
        """
        workflow_params_hash = get_params_hash(workflow_params)
        row = cls(
            workflow_name,
            workflow_params_hash,
            reference_date,
            scheduled_start_time,
            state,
        )
        session.add(row)
        session.commit()

    @classmethod
    def get_most_recent_state(
        cls,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        reference_date: "datetime.date",
        session: "sqlalchemy.orm.session.Session",
    ) -> Optional[RunState]:
        """
        Get the most recent state for a given (workflow_name, workflow_params, reference_date) combination.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        workflow_params : dict
            Parameters passed when running the workflow
        reference_date : date
            The date with which the workflow run is associated
        session : Session
            A sqlalchemy session for a DB in which this model exists.

        Returns
        -------
        RunState or None
            Most recent state, or None if no state has been set.
        """
        workflow_params_hash = get_params_hash(workflow_params)
        most_recent_row = (
            session.query(cls)
            .filter(
                cls.workflow_name == workflow_name,
                cls.workflow_params_hash == workflow_params_hash,
                cls.reference_date == reference_date,
            )
            .order_by(cls.timestamp.desc())
            .first()
        )
        if most_recent_row is not None:
            return most_recent_row.state
        else:
            return None

    @classmethod
    def can_process(
        cls,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        reference_date: "datetime.date",
        session: "sqlalchemy.orm.session.Session",
    ) -> bool:
        """
        Determine if a given (workflow_name, workflow_params, reference_date) combination is OK to process.
        Should process if we have never seen this combination or if its current state is 'failed'.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        workflow_params : dict
            Parameters passed when running the workflow
        reference_date : date
            The date with which the workflow run is associated
        session : Session
            A sqlalchemy session for a DB in which this model exists.

        Returns
        -------
        bool
            OK to process?
        """
        most_recent = cls.get_most_recent_state(
            workflow_name, workflow_params, reference_date, session
        )
        return (most_recent is None) or (most_recent == RunState.failed)

    @classmethod
    def is_done(
        cls,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        reference_date: "datetime.date",
        session: "sqlalchemy.orm.session.Session",
    ) -> bool:
        """
        Determine if a given (workflow_name, workflow_params, reference_date) combination is in 'done' state.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        workflow_params : dict
            Parameters passed when running the workflow
        reference_date : date
            The date with which the workflow run is associated
        session : Session
            A sqlalchemy session for a DB in which this model exists.

        Returns
        -------
        bool
            Done?
        """
        most_recent = cls.get_most_recent_state(
            workflow_name, workflow_params, reference_date, session
        )
        return most_recent == RunState.done


def init_db(db_uri: str, force: bool = False) -> None:
    """
    Initialise the database, optionally wipe any existing one first.

    Parameters
    ----------
    db_uri : str
        Database URI
    force : bool
        If set to true, wipes any existing database.
    """
    # db_uri = getenv("AUTOMATION_DB_URI", "sqlite:////tmp/test.db")
    # db_uri = db_uri.format(getenv("AUTOMATION_DB_PASSWORD", ""))
    engine = create_engine(db_uri)
    if force:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
