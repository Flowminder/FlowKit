# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import pendulum
import prefect
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.state import Success
from prefect.schedules import CronSchedule
from prefect.tasks.core.function import FunctionTask
from prefect.utilities.configuration import set_temporary_config
from unittest.mock import Mock, create_autospec

from autoflow.model import RunState
from autoflow.sensor import (
    add_dates_to_parameters,
    available_dates_sensor,
    filter_dates,
    get_available_dates,
    record_workflow_run_state,
    run_workflow,
    skip_if_already_run,
    WorkflowConfig,
)


def test_workflow_config_defaults():
    """
    Test that WorkflowConfig attributes earliest_date and date_stencil default to None.
    """
    workflow_config = WorkflowConfig(
        workflow=prefect.Flow(name="DUMMY_FLOW"),
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
    )
    assert workflow_config.earliest_date is None
    assert workflow_config.date_stencil is None


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


def test_filter_dates_no_filter(test_logger):
    """
    Test that the filter_dates task does not filter dates if both earliest_date and date_stencil are None.
    """
    dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )
    workflow_config = WorkflowConfig(
        workflow=prefect.Flow(name="DUMMY_FLOW"),
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=None,
        date_stencil=None,
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates.run(
            available_dates=dates, workflow_config=workflow_config
        )
    assert not filtered_dates is dates
    assert filtered_dates == dates


def test_filter_dates_by_earliest_date(test_logger):
    """
    Test that the filter_dates task removes dates before workflow_config.earliest_date.
    """
    dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )
    workflow_config = WorkflowConfig(
        workflow=prefect.Flow(name="DUMMY_FLOW"),
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=pendulum.date(2016, 1, 4),
        date_stencil=None,
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates.run(
            available_dates=dates, workflow_config=workflow_config
        )
    assert filtered_dates == list(
        pendulum.period(pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )


def test_filter_dates_by_stencil(test_logger):
    """
    Test that filter_dates removes dates for which workflow_config.date_stencil is not available.
    """
    dates = [pendulum.date(2016, 1, d) for d in [1, 3, 4, 5, 6]]
    workflow_config = WorkflowConfig(
        workflow=prefect.Flow(name="DUMMY_FLOW"),
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=None,
        date_stencil=[-2, 0],
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates.run(
            available_dates=dates, workflow_config=workflow_config
        )
    assert filtered_dates == [pendulum.date(2016, 1, d) for d in [3, 5, 6]]


def test_filter_dates_by_earliest_and_stencil(test_logger):
    """
    Test that filter_dates filters using both earliest_date and date_stencil, if both are provided.
    """
    dates = [pendulum.date(2016, 1, d) for d in [1, 3, 4, 5, 6]]
    workflow_config = WorkflowConfig(
        workflow=prefect.Flow(name="DUMMY_FLOW"),
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=pendulum.date(2016, 1, 4),
        date_stencil=[-2, 0],
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates.run(
            available_dates=dates, workflow_config=workflow_config
        )
    assert filtered_dates == [pendulum.date(2016, 1, d) for d in [5, 6]]


@pytest.mark.parametrize(
    "state,is_skipped",
    [
        (RunState.running, True),
        (RunState.success, True),
        (RunState.failed, False),
        (None, False),
    ],
)
def test_skip_if_already_run(monkeypatch, test_logger, state, is_skipped):
    """
    Test that the skip_if_already_run task skips if the workflow's most recent
    state is 'running' or 'success', and does not skip if the state is
    None (i.e. not run before) or 'failed'.
    """
    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    get_most_recent_state_mock = Mock(return_value=state)
    monkeypatch.setattr("autoflow.sensor.get_session", get_session_mock)
    monkeypatch.setattr(
        "autoflow.sensor.WorkflowRuns.get_most_recent_state", get_most_recent_state_mock
    )

    runner = TaskRunner(task=skip_if_already_run)
    upstream_edge = Edge(
        prefect.Task(), skip_if_already_run, key="parametrised_workflow"
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
        task_state = runner.run(
            upstream_states={
                upstream_edge: Success(
                    result=(
                        prefect.Flow(name="DUMMY_WORFLOW_NAME"),
                        {"DUMMY_PARAM": "DUMMY_VALUE"},
                    )
                )
            },
            context=dict(logger=test_logger),
        )

    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    get_most_recent_state_mock.assert_called_once_with(
        workflow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        session=session_mock,
    )
    session_mock.close.assert_called_once()
    assert task_state.is_successful()
    assert is_skipped == task_state.is_skipped()


def test_skip_if_already_run_unrecognised_state(monkeypatch, test_logger):
    """
    Test that skip_if_already_run raises a ValueError if get_most_recent_state
    returns an unrecognised state.
    """
    monkeypatch.setattr("autoflow.sensor.get_session", Mock())
    monkeypatch.setattr(
        "autoflow.sensor.WorkflowRuns.get_most_recent_state",
        Mock(return_value="BAD_STATE"),
    )

    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        logger=test_logger
    ), pytest.raises(ValueError, match="Unrecognised workflow state: 'BAD_STATE'"):
        skip_if_already_run.run(
            (prefect.Flow(name="DUMMY_WORFLOW_NAME"), {"DUMMY_PARAM": "DUMMY_VALUE"})
        )


def test_add_dates_to_parameters(test_logger):
    """
    Test that add_dates_to_parameters correctly combines workflow parameters with dates,
    and uses the default date stencil [0] if none is provided.
    """
    workflow_configs = [
        WorkflowConfig(
            workflow=prefect.Flow(name="WORKFLOW_1"), parameters={"DUMMY_PARAM": 1}
        ),
        WorkflowConfig(
            workflow=prefect.Flow(name="WORKFLOW_2"),
            parameters={"DUMMY_PARAM": 2},
            date_stencil=[-1, 0],
        ),
    ]
    lists_of_dates = [
        [
            pendulum.date(2016, 1, 1),
            pendulum.date(2016, 1, 2),
            pendulum.date(2016, 1, 3),
        ],
        [pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 5)],
    ]

    with prefect.context(logger=test_logger):
        parametrised_workflows = add_dates_to_parameters.run(
            workflow_configs=workflow_configs, lists_of_dates=lists_of_dates
        )

    assert len(parametrised_workflows) == 5
    assert (
        parametrised_workflows[0][0]
        == parametrised_workflows[1][0]
        == parametrised_workflows[2][0]
        == workflow_configs[0].workflow
    )
    assert (
        parametrised_workflows[3][0]
        == parametrised_workflows[4][0]
        == workflow_configs[1].workflow
    )
    for i, refdate in enumerate(lists_of_dates[0]):
        assert parametrised_workflows[i][1] == {
            "DUMMY_PARAM": 1,
            "reference_date": refdate,
            "date_ranges": [(refdate, refdate)],
        }
    for i in [3, 4]:
        assert parametrised_workflows[i][1] == {
            "DUMMY_PARAM": 2,
            "reference_date": pendulum.date(2016, 1, i + 1),
            "date_ranges": [
                (pendulum.date(2016, 1, i), pendulum.date(2016, 1, i)),
                (pendulum.date(2016, 1, i + 1), pendulum.date(2016, 1, i + 1)),
            ],
        }


def test_record_workflow_run_state(monkeypatch, test_logger):
    """
    Test that the record_workflow_run_state task calls WorkflowRuns.set_state with the correct arguments.
    """
    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.sensor.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.sensor.WorkflowRuns.set_state", set_state_mock)
    dummy_parametrised_workflow = (
        prefect.Flow(name="DUMMY_FLOW"),
        {"DUMMY_PARAM": "DUMMY_VALUE"},
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        logger=test_logger
    ):
        record_workflow_run_state.run(
            parametrised_workflow=dummy_parametrised_workflow, state=RunState.success
        )
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    set_state_mock.assert_called_once_with(
        workflow_name="DUMMY_FLOW",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        state=RunState.success,
        session=session_mock,
    )


def test_run_workflow(test_logger):
    """
    Test that the run_workflow task runs a workflow with the given parameters.
    """
    function_mock = create_autospec(lambda dummy_param: None)

    with prefect.Flow("Dummy workflow") as dummy_workflow:
        dummy_param = prefect.Parameter("dummy_param")
        FunctionTask(function_mock)(dummy_param=dummy_param)

    runner = TaskRunner(task=run_workflow)
    upstream_edge = Edge(prefect.Task(), run_workflow, key="parametrised_workflow")
    task_state = runner.run(
        upstream_states={
            upstream_edge: Success(
                result=(dummy_workflow, dict(dummy_param="DUMMY_VALUE"))
            )
        },
        context=dict(logger=test_logger),
    )
    assert task_state.is_successful()
    function_mock.assert_called_once_with(dummy_param="DUMMY_VALUE")


def test_run_workflow_fails(test_logger):
    """
    Test that the run_workflow task fails if the workflow fails.
    """
    function_mock = create_autospec(
        lambda dummy_param: None, side_effect=Exception("Workflow failed")
    )

    with prefect.Flow("Dummy workflow") as dummy_workflow:
        dummy_param = prefect.Parameter("dummy_param")
        FunctionTask(function_mock)(dummy_param=dummy_param)

    runner = TaskRunner(task=run_workflow)
    upstream_edge = Edge(prefect.Task(), run_workflow, key="parametrised_workflow")
    task_state = runner.run(
        upstream_states={
            upstream_edge: Success(
                result=(dummy_workflow, dict(dummy_param="DUMMY_VALUE"))
            )
        },
        context=dict(logger=test_logger),
    )
    assert task_state.is_failed()


def test_run_workflow_ignores_schedule(test_logger):
    """
    Test that run_workflow ignores the workflow's schedule.
    """
    function_mock = create_autospec(lambda dummy_param: None)
    # Flow with no more scheduled runs
    with prefect.Flow(
        "Dummy_workflow",
        schedule=CronSchedule("0 0 * * *", end_date=pendulum.now().subtract(days=2)),
    ) as dummy_workflow:
        dummy_param = prefect.Parameter("dummy_param")
        FunctionTask(function_mock)(dummy_param=dummy_param)

    with prefect.context(logger=test_logger):
        run_workflow.run(
            parametrised_workflow=(dummy_workflow, dict(dummy_param="DUMMY_VALUE"))
        )
    function_mock.assert_called_once_with(dummy_param="DUMMY_VALUE")


# TODO: Add tests for available_dates_sensor. Maybe use bits of these tests:
# def test_record_workflow_done_upstream_success(monkeypatch, test_logger):
#     """
#     Test that the record_workflow_done task runs if all of its upstream tasks are successful.
#     """
#     runner = TaskRunner(task=record_workflow_done)
#     edge1 = Edge(Task(), record_workflow_done)
#     edge2 = Edge(Task(), record_workflow_done)
#     set_state_mock = Mock()
#     monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
#     monkeypatch.setattr("autoflow.tasks.get_session", Mock())
#     context = dict(
#         flow_name="DUMMY_WORFLOW_NAME",
#         parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
#         scheduled_start_time=pendulum.datetime(2016, 1, 1),
#         logger=test_logger,
#     )
#     with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
#         final_state = runner.run(
#             upstream_states={edge1: Success(), edge2: Success()}, context=context
#         )
#     assert final_state.is_successful()
#     set_state_mock.assert_called_once()
#
# def test_record_workflow_done_upstream_failed(monkeypatch, test_logger):
#     """
#     Test that the record_workflow_done task does not run if any of its upstream tasks fail.
#     """
#     runner = TaskRunner(task=record_workflow_done)
#     edge1 = Edge(Task(), record_workflow_done)
#     edge2 = Edge(Task(), record_workflow_done)
#     set_state_mock = Mock()
#     monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
#     monkeypatch.setattr("autoflow.tasks.get_session", Mock())
#     context = dict(
#         flow_name="DUMMY_WORFLOW_NAME",
#         parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
#         scheduled_start_time=pendulum.datetime(2016, 1, 1),
#         logger=test_logger,
#     )
#     with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
#         final_state = runner.run(
#             upstream_states={edge1: Success(), edge2: Failed()}, context=context
#         )
#     assert isinstance(final_state, TriggerFailed)
#     set_state_mock.assert_not_called()
#
# def test_record_workflow_failed_upstream_success(monkeypatch, test_logger):
#     """
#     Test that the record_workflow_failed task does not run if all of its upstream tasks are successful.
#     """
#     runner = TaskRunner(task=record_workflow_failed)
#     edge1 = Edge(Task(), record_workflow_failed)
#     edge2 = Edge(Task(), record_workflow_failed)
#     set_state_mock = Mock()
#     monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
#     monkeypatch.setattr("autoflow.tasks.get_session", Mock())
#     context = dict(
#         flow_name="DUMMY_WORFLOW_NAME",
#         parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
#         scheduled_start_time=pendulum.datetime(2016, 1, 1),
#         logger=test_logger,
#     )
#     with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
#         final_state = runner.run(
#             upstream_states={edge1: Success(), edge2: Success()}, context=context
#         )
#     assert isinstance(final_state, TriggerFailed)
#     set_state_mock.assert_not_called()
#
# def test_record_workflow_failed_upstream_failed(monkeypatch, test_logger):
#     """
#     Test that the record_workflow_failed task runs if any of its upstream tasks fail.
#     """
#     runner = TaskRunner(task=record_workflow_failed)
#     edge1 = Edge(Task(), record_workflow_failed)
#     edge2 = Edge(Task(), record_workflow_failed)
#     set_state_mock = Mock()
#     monkeypatch.setattr("autoflow.tasks.WorkflowRuns.set_state", set_state_mock)
#     monkeypatch.setattr("autoflow.tasks.get_session", Mock())
#     context = dict(
#         flow_name="DUMMY_WORFLOW_NAME",
#         parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
#         scheduled_start_time=pendulum.datetime(2016, 1, 1),
#         logger=test_logger,
#     )
#     with set_temporary_config({"db_uri": "DUMMY_DB_URI"}):
#         final_state = runner.run(
#             upstream_states={edge1: Success(), edge2: Failed()}, context=context
#         )
#     assert final_state.is_successful()
#     set_state_mock.assert_called_once()
