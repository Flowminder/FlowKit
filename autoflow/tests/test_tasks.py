# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from prefect import Task
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.state import Success

from autoflow.tasks import mappable_dict


def test_mappable_dict_returns_dict():
    """
    Test that the result of running the mappable_dict task is a dict of its keyword argunments.
    """
    kwargs = dict(a=1, b=2, c=3)
    task_result = mappable_dict.run(**kwargs)
    assert isinstance(task_result, dict)
    assert task_result == kwargs


def test_mappable_dict_can_be_mapped():
    """
    Test that the mappable_dict task can be mapped over inputs.
    """
    runner = TaskRunner(task=mappable_dict)
    mapped_edge = Edge(Task(), mappable_dict, key="mapped_arg", mapped=True)
    unmapped_edge = Edge(Task(), mappable_dict, key="unmapped_arg", mapped=False)
    final_state = runner.run(
        upstream_states={
            mapped_edge: Success(result=[1, 2]),
            unmapped_edge: Success(result=[3, 4]),
        }
    )
    assert final_state.is_successful()
    assert final_state.is_mapped()
    assert final_state.map_states[0].result == {"mapped_arg": 1, "unmapped_arg": [3, 4]}
    assert final_state.map_states[1].result == {"mapped_arg": 2, "unmapped_arg": [3, 4]}
