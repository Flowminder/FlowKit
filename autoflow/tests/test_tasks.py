# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import prefect
import pendulum
from unittest.mock import Mock, call
from prefect.utilities.configuration import set_temporary_config
from prefect import Task
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.state import Success, Failed, TriggerFailed

from autoflow.tasks import *
from autoflow.utils import get_params_hash


def test_get_tag():
    """
    Test that the get_tag task returns the expected tag.
    """
    dummy_params = {"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"}
    with prefect.context(parameters=dummy_params, flow_name="DUMMY-FLOW-NAME"):
        tag = get_tag.run()
    assert tag == f"DUMMY-FLOW-NAME_{get_params_hash(dummy_params)}"


def test_get_tag_with_date():
    """
    Test that the get_tag task returns the expected tag when a reference date is provided.
    """
    dummy_params = {"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"}
    with prefect.context(parameters=dummy_params, flow_name="DUMMY-FLOW-NAME"):
        tag = get_tag.run(reference_date=pendulum.date(2016, 1, 1))
    assert tag == f"DUMMY-FLOW-NAME_{get_params_hash(dummy_params)}_2016-01-01"


def test_get_date_ranges(monkeypatch):
    """
    Test that the get_date_ranges task returns the result of stencil_to_date_pairs.
    """
    stencil_to_date_pairs_mock = Mock(return_value="DUMMY_DATE_PAIRS")
    monkeypatch.setattr(
        "autoflow.tasks.stencil_to_date_pairs", stencil_to_date_pairs_mock
    )
    reference_date = pendulum.date(2016, 1, 1)
    date_stencil = [-1, 0]
    date_ranges = get_date_ranges.run(
        reference_date=reference_date, date_stencil=date_stencil
    )
    assert date_ranges == "DUMMY_DATE_PAIRS"
    stencil_to_date_pairs_mock.assert_called_once_with(
        stencil=date_stencil, reference_date=reference_date
    )


def test_get_date_ranges_default():
    """
    Test that if no stencil is provided, the get_date_ranges task returns a single date range containing only the reference date.
    """
    reference_date = pendulum.date(2016, 1, 1)
    date_ranges = get_date_ranges.run(reference_date=reference_date)
    assert date_ranges == [(reference_date, reference_date)]


def test_get_flowapi_url():
    """
    Test that the get_flowapi_url task returns the FlowAPI URL set in the prefect config.
    """
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}):
        flowapi_url = get_flowapi_url.run()
    assert flowapi_url == "DUMMY_URL"


def test_get_available_dates(monkeypatch, test_logger):
    """
    Test that get_available_dates gets available dates using FlowClient and returns as date objects in a sorted list.
    """
    flowclient_available_dates = {
        "cdr_type_1": ["2016-01-01", "2016-01-03"],
        "cdr_type_2": ["2016-01-01", "2016-01-02"],
    }
    connect_mock = Mock()
    monkeypatch.setattr("flowclient.connect", connect_mock)
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )
    monkeypatch.setenv("FLOWAPI_TOKEN", "DUMMY_TOKEN")
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}), prefect.context(
        logger=test_logger
    ):
        dates = get_available_dates.run()
    connect_mock.assert_called_once_with(url="DUMMY_URL", token="DUMMY_TOKEN")
    assert dates == [
        pendulum.date(2016, 1, 1),
        pendulum.date(2016, 1, 2),
        pendulum.date(2016, 1, 3),
    ]


def test_get_available_dates_cdr_types(monkeypatch, test_logger):
    """
    Test that get_available_dates can get available dates for a subset of CDR types.
    """
    flowclient_available_dates = {
        "cdr_type_1": ["2016-01-01", "2016-01-03"],
        "cdr_type_2": ["2016-01-01", "2016-01-02"],
        "cdr_type_3": ["2016-01-03", "2016-01-04"],
    }
    monkeypatch.setattr("flowclient.connect", Mock())
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )
    monkeypatch.setenv("FLOWAPI_TOKEN", "DUMMY_TOKEN")
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}), prefect.context(
        logger=test_logger
    ):
        dates = get_available_dates.run(cdr_types=["cdr_type_1", "cdr_type_2"])
    assert dates == [
        pendulum.date(2016, 1, 1),
        pendulum.date(2016, 1, 2),
        pendulum.date(2016, 1, 3),
    ]


def test_get_available_dates_warns(monkeypatch, test_logger):
    """
    Test that get_available_dates warns if no dates are available for a specified CDR type,
    but still returns the available dates for other specified CDR types.
    """
    flowclient_available_dates = {
        "cdr_type_1": ["2016-01-01", "2016-01-03"],
        "cdr_type_2": ["2016-01-01", "2016-01-02"],
    }
    monkeypatch.setattr("flowclient.connect", Mock())
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )
    monkeypatch.setenv("FLOWAPI_TOKEN", "DUMMY_TOKEN")
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}), prefect.context(
        logger=test_logger
    ), pytest.warns(
        UserWarning, match="No data available for CDR types {'cdr_type_3'}."
    ):
        dates = get_available_dates.run(cdr_types=["cdr_type_1", "cdr_type_3"])
    assert dates == [pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 3)]


def test_filter_dates_by_earliest_date(test_logger):
    """
    Test that the filter_dates_by_earliest_date task removes dates before earliest_date.
    """
    dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )
    earliest_date = pendulum.date(2016, 1, 4)
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates_by_earliest_date.run(
            dates, earliest_date=earliest_date
        )
    assert filtered_dates == list(
        pendulum.period(pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )


def test_filter_dates_by_earliest_date_default(test_logger):
    """
    Test that the filter_dates_by_earliest_date task does not filter dates if no earliest_date is provided.
    """
    dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates_by_earliest_date.run(dates)
    assert filtered_dates == dates


def test_filter_dates_by_stencil(test_logger):
    """
    Test that filter_dates_by_stencil removes dates for which stencil is not available.
    """
    dates = [pendulum.date(2016, 1, d) for d in [3, 4, 5]]
    available_dates = [pendulum.date(2016, 1, d) for d in [1, 3, 4, 5, 6]]
    date_stencil = [-2, 0]
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates_by_stencil.run(
            dates=dates, available_dates=available_dates, date_stencil=date_stencil
        )
    assert filtered_dates == [pendulum.date(2016, 1, 3), pendulum.date(2016, 1, 5)]


def test_filter_dates_by_stencil_default(test_logger):
    """
    Test that filter_dates_by_stencil does not filter dates if no stencil is provided.
    """
    dates = [pendulum.date(2016, 1, d) for d in [3, 4, 5]]
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates_by_stencil.run(dates=dates, available_dates=[])
    assert filtered_dates == dates


def test_filter_dates_by_previous_runs(monkeypatch, test_logger):
    """
    Test that the filter_dates_by_previous_runs removes dates for which WorkFlowRuns.can_process returns False.
    """
    dates = [pendulum.date(2016, 1, d) for d in [1, 2, 3]]

    def dummy_can_process(workflow_name, workflow_params, reference_date, session):
        assert workflow_name == "DUMMY_WORFLOW_NAME"
        assert workflow_params == {"DUMMY_PARAM": "DUMMY_VALUE"}
        if reference_date in [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)]:
            return True
        else:
            return False

    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.can_process", dummy_can_process)
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        logger=test_logger,
    ):
        filtered_dates = filter_dates_by_previous_runs.run(dates)
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    assert filtered_dates == [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)]


@pytest.mark.parametrize(
    "record_task,state",
    [
        (record_workflow_in_process, "in_process"),
        (record_workflow_done, "done"),
        (record_workflow_failed, "failed"),
    ],
)
def test_record_workflow_state_tasks(monkeypatch, test_logger, record_task, state):
    """
    Test that the record_workflow_* tasks call WorkflowRuns.set_state with the correct state.
    """
    reference_date = pendulum.date(2016, 1, 1)
    scheduled_start_time = pendulum.datetime(2016, 1, 1)
    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=scheduled_start_time,
        logger=test_logger,
    ):
        record_task.run(reference_date)
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    set_state_mock.assert_called_once_with(
        workflow_name="DUMMY_WORFLOW_NAME",
        workflow_params={"DUMMY_PARAM": "DUMMY_VALUE"},
        reference_date=reference_date,
        scheduled_start_time=scheduled_start_time,
        state=state,
        session=session_mock,
    )


@pytest.mark.parametrize(
    "record_task,state",
    [
        (record_workflow_in_process, "in_process"),
        (record_workflow_done, "done"),
        (record_workflow_failed, "failed"),
    ],
)
def test_record_workflow_state_tasks_no_reference_date(
    monkeypatch, test_logger, record_task, state
):
    """
    Test that the record_workflow_* tasks can be called without a reference date.
    """
    scheduled_start_time = pendulum.datetime(2016, 1, 1)
    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=scheduled_start_time,
        logger=test_logger,
    ):
        record_task.run()
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    set_state_mock.assert_called_once_with(
        workflow_name="DUMMY_WORFLOW_NAME",
        workflow_params={"DUMMY_PARAM": "DUMMY_VALUE"},
        reference_date=None,
        scheduled_start_time=scheduled_start_time,
        state=state,
        session=session_mock,
    )


def test_record_workflow_done_upstream_success(monkeypatch, test_logger):
    """
    Test that the record_workflow_done task runs if all of its upstream tasks are successful.
    """
    runner = TaskRunner(task=record_workflow_done)
    edge1 = Edge(Task(), record_workflow_done)
    edge2 = Edge(Task(), record_workflow_done)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.get_session", Mock())
    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={edge1: Success(), edge2: Success()}, context=context
        )
    assert final_state.is_successful()
    set_state_mock.assert_called_once()


def test_record_workflow_done_upstream_failed(monkeypatch, test_logger):
    """
    Test that the record_workflow_done task does not run if any of its upstream tasks fail.
    """
    runner = TaskRunner(task=record_workflow_done)
    edge1 = Edge(Task(), record_workflow_done)
    edge2 = Edge(Task(), record_workflow_done)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.get_session", Mock())
    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={edge1: Success(), edge2: Failed()}, context=context
        )
    assert isinstance(final_state, TriggerFailed)
    set_state_mock.assert_not_called()


def test_record_workflow_failed_upstream_success(monkeypatch, test_logger):
    """
    Test that the record_workflow_failed task does not run if all of its upstream tasks are successful.
    """
    runner = TaskRunner(task=record_workflow_failed)
    edge1 = Edge(Task(), record_workflow_failed)
    edge2 = Edge(Task(), record_workflow_failed)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.get_session", Mock())
    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={edge1: Success(), edge2: Success()}, context=context
        )
    assert isinstance(final_state, TriggerFailed)
    set_state_mock.assert_not_called()


def test_record_workflow_failed_upstream_failed(monkeypatch, test_logger):
    """
    Test that the record_workflow_failed task runs if any of its upstream tasks fail.
    """
    runner = TaskRunner(task=record_workflow_failed)
    edge1 = Edge(Task(), record_workflow_failed)
    edge2 = Edge(Task(), record_workflow_failed)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.get_session", Mock())
    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={edge1: Success(), edge2: Failed()}, context=context
        )
    assert final_state.is_successful()
    set_state_mock.assert_called_once()


def test_record_any_failed_workflows(monkeypatch, test_logger):
    """
    Test that the record_any_failed_workflows task sets state "failed" for any reference dates
    for which WorkflowRuns.is_done returns False.
    """
    runner = TaskRunner(task=record_any_failed_workflows)
    reference_dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 5)).range(
            "days"
        )
    )
    dates_edge = Edge(Task(), record_any_failed_workflows, key="reference_dates")

    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    is_done_mock = Mock(side_effect=[True, True, False, False, True])
    monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.is_done", is_done_mock)

    scheduled_start_time = pendulum.datetime(2016, 2, 1)
    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=scheduled_start_time,
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={dates_edge: Success(result=reference_dates)},
            context=context,
        )

    is_done_mock.assert_has_calls(
        [
            call(
                workflow_name="DUMMY_WORFLOW_NAME",
                workflow_params={"DUMMY_PARAM": "DUMMY_VALUE"},
                reference_date=date,
                session=session_mock,
            )
            for date in reference_dates
        ]
    )
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    assert set_state_mock.call_count == 2
    set_state_mock.assert_has_calls(
        [
            call(
                workflow_name="DUMMY_WORFLOW_NAME",
                workflow_params={"DUMMY_PARAM": "DUMMY_VALUE"},
                reference_date=date,
                scheduled_start_time=scheduled_start_time,
                state="failed",
                session=session_mock,
            )
            for date in [pendulum.date(2016, 1, 3), pendulum.date(2016, 1, 4)]
        ]
    )
    assert final_state.is_failed()


def test_record_any_failed_workflows_succeeds_if_nothing_failed(
    monkeypatch, test_logger
):
    """
    Test that the record_any_workflows_failed task does not call WorkflowRuns.set_state
    if all workflow runs are in "done" state.
    """
    runner = TaskRunner(task=record_any_failed_workflows)
    reference_dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 5)).range(
            "days"
        )
    )
    dates_edge = Edge(Task(), record_any_failed_workflows, key="reference_dates")

    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    is_done_mock = Mock(return_value=True)
    monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.is_done", is_done_mock)

    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={dates_edge: Success(result=reference_dates)},
            context=context,
        )

    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    set_state_mock.assert_not_called()
    assert final_state.is_successful()


@pytest.mark.parametrize("upstream_state", [Success(), Failed()])
def test_record_any_failed_workflows_always_runs(
    monkeypatch, test_logger, upstream_state
):
    """
    Test that the record_any_workflows_failed task always runs, whether its upstream tasks succeed or fail.
    """
    runner = TaskRunner(task=record_any_failed_workflows)
    reference_dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 5)).range(
            "days"
        )
    )
    dates_edge = Edge(Task(), record_any_failed_workflows, key="reference_dates")
    upstream_task_edge = Edge(Task(), record_any_failed_workflows)

    is_done_mock = Mock(return_value=True)
    monkeypatch.setattr("autoflow.tasks.get_session", Mock())
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", Mock())
    monkeypatch.setattr("autoflow.tasks.WorkflowRuns.is_done", is_done_mock)

    context = dict(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        scheduled_start_time=pendulum.datetime(2016, 1, 1),
        logger=test_logger,
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        final_state = runner.run(
            upstream_states={
                dates_edge: Success(result=reference_dates),
                upstream_task_edge: upstream_state,
            },
            context=context,
        )

    is_done_mock.assert_called()
    assert not isinstance(final_state, TriggerFailed)


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


# def test_papermill_execute_notebook(monkeypatch, test_logger):
#     """
#     """
#     execute_notebook_mock = Mock()
#     monkeypatch.setattr("papermill.execute_notebook", execute_notebook_mock)
#     with prefect.context(logger=test_logger):
