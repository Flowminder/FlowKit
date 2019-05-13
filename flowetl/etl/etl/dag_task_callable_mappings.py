# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mapping task id to a python callable. Allows for the specification of a set of
dummy callables to be used for testing.
"""

from etl.dummy_task_callables import dummy_callable, dummy_failing_callable
from etl.production_task_callables import success_branch_callable

# callables to be used when testing the structure of the ETL DAG
TEST_TASK_CALLABLES = {
    "init": dummy_callable,
    "extract": dummy_callable,
    "transform": dummy_callable,
    "load": dummy_callable,
    "success_branch": success_branch_callable,
    "archive": dummy_callable,
    "quarantine": dummy_callable,
    "clean": dummy_callable,
    "fail": dummy_failing_callable,
}

# callables to be used in production
PRODUCTION_TASK_CALLABLES = {
    "init": dummy_callable,
    "extract": dummy_callable,
    "transform": dummy_callable,
    "load": dummy_callable,
    "success_branch": success_branch_callable,
    "archive": dummy_callable,
    "quarantine": dummy_callable,
    "clean": dummy_callable,
    "fail": dummy_failing_callable,
}
