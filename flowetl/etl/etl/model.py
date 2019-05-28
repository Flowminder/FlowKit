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

from etl import etl_utils

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
    cdr_type = Column(String)
    cdr_date = Column(Date)
    state = Column(String)
    timestamp = Column(DateTime(timezone=True))

    def __init__(self, *, cdr_type: str, cdr_date: pendulumDate, state: str):

        self.cdr_type = etl_utils.CDRType(cdr_type)
        self.cdr_date = cdr_date
        self.state = etl_utils.State(state)
        self.timestamp = pendulum.utcnow()

    @classmethod
    def set_state(
        cls, *, cdr_type: str, cdr_date: pendulumDate, state: str, session: Session
    ) -> None:
        """
        Add new row to the etl book-keeping table.

        Parameters
        ----------
        cdr_type : str
            CDR type of file being processed ("calls", "sms", "mds" or "topups")
        cdr_date : Date
            The date with which the file's data is associated
        state : str
            The state in the ingestion process the file currently
            is ("ingest", "quarantine" or "archive")
        session : Session
            A sqlalchemy session for a DB in which this model exists.
        """
        row = cls(cdr_type=cdr_type, cdr_date=cdr_date, state=state)
        session.add(row)
        session.commit()

    @classmethod
    def can_process(cls, *, cdr_type: str, cdr_date: pendulumDate, session: Session):
        """
        Method that determines if a given cdr_type, cdr_date pair is ok to process.
        If we have never seen the pair then should process or if pair has been seen but
        its current state is quarantine.

        Parameters
        ----------
        cdr_type : str
            The type of the CDR data
        cdr_date : pendulumDate
            The date of the CDR data
        session : Session
            A sqlalchemy session for a DB in which this model exists.

        Returns
        -------
        bool
            OK to process the pair?
        """
        res = (
            session.query(cls)
            .filter(cls.cdr_type == cdr_type, cls.cdr_date == cdr_date)
            .order_by(cls.timestamp.desc())
            .first()
        )

        if (res is None) or (res.state == "quarantine"):
            process = True
        else:
            process = False

        return process
