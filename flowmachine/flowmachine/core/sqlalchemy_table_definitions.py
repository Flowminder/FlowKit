# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from sqlalchemy import Column, Text, Boolean, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base

__all__ = ["EventsCallsTable"]

Base = declarative_base()


class EventsCallsTable(Base):
    __tablename__ = "calls"
    __table_args__ = {"schema": "events"}

    id = Column("id", Text(), primary_key=True)
    outgoing = Column("outgoing", Boolean())
    datetime = Column("datetime", TIMESTAMP(timezone=True), nullable=False)
    duration = Column("duration", Numeric())
    network = Column("network", Text())
    msisdn = Column("msisdn", Text(), nullable=False)
    msisdn_counterpart = Column("msisdn_counterpart", Text())
    location_id = Column("location_id", Text())
    imsi = Column("imsi", Text())
    imei = Column("imei", Text())
    tac = Column("tac", Numeric(precision=8, scale=0))
    operator_code = Column("operator_code", Numeric())
    country_code = Column("country_code", Numeric())
