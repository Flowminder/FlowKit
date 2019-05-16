# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Define a DB model for storing the process of ingestion
"""

import pendulum
from pendulum.date import Date as pendulumDate

from sqlalchemy import Column, String, DateTime, Date, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

Base = declarative_base()

# pylint: disable=too-few-public-methods
class ETLRecord(Base):
    """
    DB Model for storing the process of ingestion
    for found files.
    """

    __tablename__ = "etl_records"
    __table_args__ = {"schema": "etl"}

    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    cdr_type = Column(String)
    cdr_date = Column(Date)
    state = Column(String)
    timestamp = Column(DateTime)

    def __init__(
        self, *, file_name: str, cdr_type: str, cdr_date: pendulumDate, state: str
    ):

        # This should be set more globally - not using enums
        # because don't have a migration strategy in place
        allowed_states = ["ingest", "archive", "quarantine"]
        allowed_cdr_types = ["calls", "sms", "mds", "topups"]
        # pylint: disable=no-else-raise
        if state not in allowed_states or cdr_type not in allowed_cdr_types:
            raise ValueError(
                f"state should be one of {allowed_states} and cdr_type one of {allowed_cdr_types}"
            )
        else:
            self.file_name = file_name
            self.cdr_type = cdr_type
            self.cdr_date = cdr_date
            self.state = state
            self.timestamp = pendulum.utcnow()

    @classmethod
    def set_state(
        cls,
        *,
        file_name: str,
        cdr_type: str,
        cdr_date: pendulumDate,
        state: str,
        session: Session,
    ) -> None:
        """
        Add new row to the etl book-keeping table.

        Parameters
        ----------
        file_name : str
            Name of file being processed
        cdr_type : str
            CDR type of file being processed ("calls", "sms", "mds" or "topups")
        cdr_date : Date
            The date with which the files data is associated
        state : str
            The state in the ingestion process the file currently
            is ("ingest", "quarantine" or "archive")
        session : Session
            A sqlalchmy session for a DB in which this model exists.
        """
        row = cls(
            file_name=file_name, cdr_type=cdr_type, cdr_date=cdr_date, state=state
        )
        session.add(row)
        session.commit()
