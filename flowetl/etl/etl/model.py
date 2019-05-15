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

        allowed_states = ["ingest", "archive", "quarantine"]
        if state not in allowed_states:
            raise Exception(f"State should be one of {allowed_states}")
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
