# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mapping task id to a python callable. Allows for the specification of a set of
dummy callables to be used for testing.
"""
from functools import partial

from etl.dummy_task_callables import dummy__callable, dummy_failing__callable
from etl.production_task_callables import success_branch__callable

# callables to be used when testing the structure of the ETL DAG
TEST_TASK_CALLABLES = {
    "init": dummy__callable,
    "extract": dummy__callable,
    "transform": dummy__callable,
    "load": dummy__callable,
    "success_branch": success_branch__callable,
    "archive": dummy__callable,
    "quarantine": dummy__callable,
    "clean": dummy__callable,
    "fail": dummy_failing__callable,
}

# callables to be used in production
PRODUCTION_TASK_CALLABLES = {
    "init": dummy__callable,
    "extract": dummy__callable,
    "transform": dummy__callable,
    "load": dummy__callable,
    "success_branch": success_branch__callable,
    "archive": dummy__callable,
    "quarantine": dummy__callable,
    "clean": dummy__callable,
    "fail": dummy_failing__callable,
}
