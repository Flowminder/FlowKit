# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the 'WorkflowRuns' class, which represents  the 'workflow_runs' database table for storing the state of workflow runs.
"""

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
    possible states a workflow run can be in.
    """

    running = 1
    success = 2
    failed = 3


# TODO: Add a table for looking up workflow parameters from the parameters hash


class WorkflowRuns(Base):
    """
    DB Model for storing the state of workflow runs.
    """

    __tablename__ = "workflow_runs"

    id = Column(Integer, primary_key=True)
    workflow_name = Column(String)
    parameters_hash = Column(String)
    state = Column(Enum(RunState))
    timestamp = Column(DateTime(timezone=True))

    def __init__(self, workflow_name: str, parameters_hash: str, state: RunState):
        self.workflow_name = workflow_name
        self.parameters_hash = (
            parameters_hash
        )  # Hash generated from workflow parameters dict
        self.state = state  # Flow run state
        self.timestamp = pendulum.now("utc")

    @classmethod
    def set_state(
        cls,
        workflow_name: str,
        parameters: Dict[str, Any],
        state: RunState,
        session: "sqlalchemy.orm.session.Session",
    ) -> None:
        """
        Add a new row to the workflow runs table.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        parameters : dict
            Parameters passed when running the workflow
        state : RunState
            The state of the workflow run
        session : Session
            A sqlalchemy session for a DB in which this model exists.
        """
        parameters_hash = get_params_hash(parameters)
        row = cls(workflow_name, parameters_hash, state)
        session.add(row)
        session.flush()

    @classmethod
    def get_most_recent_state(
        cls,
        workflow_name: str,
        parameters: Dict[str, Any],
        session: "sqlalchemy.orm.session.Session",
    ) -> Optional[RunState]:
        """
        Get the most recent state for a given (workflow_name, parameters) combination.

        Parameters
        ----------
        workflow_name : str
            Name of the workflow
        parameters : dict
            Parameters passed when running the workflow
        session : Session
            A sqlalchemy session for a DB in which this model exists.

        Returns
        -------
        RunState or None
            Most recent state, or None if no state has been set.
        """
        parameters_hash = get_params_hash(parameters)
        most_recent_row = (
            session.query(cls)
            .filter(
                cls.workflow_name == workflow_name,
                cls.parameters_hash == parameters_hash,
            )
            .order_by(cls.timestamp.desc())
            .first()
        )
        if most_recent_row is not None:
            return most_recent_row.state
        else:
            return None


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
    engine = create_engine(db_uri)
    if force:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
