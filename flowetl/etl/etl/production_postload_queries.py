# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import structlog
from pendulum.date import Date as pendulumDate

from etl.model import ETLPostLoadOutcome

logger = structlog.get_logger("flowetl")


# pylint: disable=unused-argument
def num_total_calls__callable(*, cdr_date: pendulumDate, **kwargs):
    """
    Function to determine the number of total calls
    """
    logger.info("got here")
