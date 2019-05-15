import pendulum
from pendulum.date import Date

from sqlalchemy import Column, String, DateTime, Date, Integer, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

Base = declarative_base()


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

    def __init__(self, *, file_name: str, cdr_type: str, cdr_date: Date, state: str):

        # This should be set more globally - not using enums
        # because don't have a migration strategy in place
        allowed_states = ["ingest", "archive", "quarantine"]
        allowed_cdr_types = ["calls", "sms", "mds", "topups"]

        if state not in allowed_states or cdr_type not in allowed_cdr_types:
            raise Exception(
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
        cdr_date: Date,
        state: str,
        session: Session,
    ) -> None:
        row = cls(
            file_name=file_name, cdr_type=cdr_type, cdr_date=cdr_date, state=state
        )
        session.add(row)
        session.commit()
