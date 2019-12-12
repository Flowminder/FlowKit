# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Queries to be run in the postload step.
"""
from functools import partial

from etl.etl_utils import CDRType
from etl.production_postload_queries import num_total_events__callable


POSTETL_QUERIES = {"num_total_events": partial(num_total_events__callable)}

POSTETL_QUERIES_FOR_TYPE = {
    CDRType.CALLS: [POSTETL_QUERIES["num_total_events"]],
    CDRType.SMS: [POSTETL_QUERIES["num_total_events"]],
    CDRType.MDS: [POSTETL_QUERIES["num_total_events"]],
    CDRType.TOPUPS: [POSTETL_QUERIES["num_total_events"]],
}
