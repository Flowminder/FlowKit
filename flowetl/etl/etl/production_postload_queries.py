# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
from sqlalchemy.orm.session import Session
from pendulum.date import Date as pendulumDate


# pylint: disable=unused-argument
def num_total_calls__callable(*, cdr_date: pendulumDate, session: Session, **kwargs):
    """
    Function to determine the number of total calls
    """
    cdr_date_tomorrow = cdr_date.add(days=1)

    sql = f"""
    SELECT COUNT(*)
    FROM events.calls
    WHERE datetime >= '{ cdr_date }' AND datetime < '{ cdr_date_tomorrow }'
    """
    outcome = session.execute(sql).fetchone()[0]

    return {
        "outcome": outcome,
        "type_of_query_or_check": "num_total_calls",
        "optional_comment_or_description": "Total number of calls",
    }
