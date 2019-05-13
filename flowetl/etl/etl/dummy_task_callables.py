# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of dummy callables to be used when testing
"""

import os
import logging

from airflow.models import DagRun, TaskInstance

# pylint: disable=unused-argument
def dummy_callable(*, dag_run: DagRun, task_instance: TaskInstance, **kwargs):
    """
    Dummy python callable - raises an exception if the environment variable
    TASK_TO_FAIL is set to the name of the current task, otherwise succeeds
    silently.
    """
    logging.info(dag_run)
    if os.environ.get("TASK_TO_FAIL", "") == task_instance.task_id:
        raise Exception


def dummy_failing_callable(*, dag_run: DagRun, **kwargs):
    """
    Dummy python callable raising an exception
    """
    logging.info(dag_run)
    raise Exception
