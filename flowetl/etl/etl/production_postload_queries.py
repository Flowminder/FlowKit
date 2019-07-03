# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import structlog
from sqlalchemy.orm.session import Session
from pendulum.date import Date as pendulumDate

from etl.model import ETLPostQueryOutcome

logger = structlog.get_logger("flowetl")


# pylint: disable=unused-argument
def num_total_calls__callable(
    *,
    cdr_date: pendulumDate,
    session: Session,
    **kwargs
):
    """
    Function to determine the number of total calls
    """

    sql = f"""
    select
        count(*)
    from
        events.calls
    """
    outcome = session.execute(sql).fetchone()[0]

    return {
        "outcome": outcome,
        "type_of_query_or_check": "num_total_calls",
        "optional_comment_or_description": "The number of total calls",
    }
