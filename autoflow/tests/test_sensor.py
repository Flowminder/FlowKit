# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from unittest.mock import call, create_autospec, Mock

import pendulum
import prefect
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.state import Failed, Success
from prefect.environments.storage import Local
from prefect.schedules import CronSchedule
from prefect.tasks.core.function import FunctionTask
from prefect.utilities.configuration import set_temporary_config

from autoflow.date_stencil import DateStencil
from autoflow.model import RunState
from autoflow.sensor import (
    available_dates_sensor,
    filter_dates,
    get_available_dates,
    get_parametrised_workflows,
    record_workflow_run_state,
    run_workflow,
    skip_if_already_run,
    WorkflowConfig,
)


def test_workflow_config_defaults():
    """
    Test that WorkflowConfig attributes 'parameters', 'earliest_date' and 'date_stencil' have the correct defaults.
    """
    workflow_config = WorkflowConfig(workflow_name="DUMMY_FLOW")
    assert workflow_config.parameters is None
    assert workflow_config.earliest_date is None
    assert workflow_config.date_stencil._intervals == ((0, 1),)


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
    connect_mock.assert_called_once_with(
        ssl_certificate=None, url="DUMMY_URL", token="DUMMY_TOKEN"
    )
    assert dates == [
        pendulum.date(2016, 1, 1),
        pendulum.date(2016, 1, 2),
        pendulum.date(2016, 1, 3),
    ]


def test_get_available_dates_ssl_certificate(monkeypatch, test_logger):
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
    monkeypatch.setenv("SSL_CERTIFICATE_FILE", "DUMMY_SSL_CERT")
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}), prefect.context(
        logger=test_logger
    ):
        get_available_dates.run()
    connect_mock.assert_called_once_with(
        url="DUMMY_URL", token="DUMMY_TOKEN", ssl_certificate="DUMMY_SSL_CERT"
    )


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
    Test that the filter_dates task does not filter dates if neither earliest_date nor date_stencil are set.
    """
    dates = list(
        pendulum.period(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 7)).range(
            "days"
        )
    )
    workflow_config = WorkflowConfig(workflow_name="DUMMY_FLOW")
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
        workflow_name="DUMMY_FLOW", earliest_date=pendulum.date(2016, 1, 4)
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
        workflow_name="DUMMY_FLOW",
        earliest_date=None,
        date_stencil=DateStencil([-2, 0]),
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
        workflow_name="DUMMY_FLOW",
        earliest_date=pendulum.date(2016, 1, 4),
        date_stencil=DateStencil([-2, 0]),
    )
    with prefect.context(logger=test_logger):
        filtered_dates = filter_dates.run(
            available_dates=dates, workflow_config=workflow_config
        )
    assert filtered_dates == [pendulum.date(2016, 1, d) for d in [5, 6]]


def test_get_parametrised_workflows(test_logger, tmpdir):
    """
    Test that get_parametrised_workflows correctly combines workflow parameters with dates.
    """
    workflow_storage = Local(tmpdir)
    workflow_storage.add_flow(prefect.Flow(name="WORKFLOW_1"))
    workflow_storage.add_flow(prefect.Flow(name="WORKFLOW_2"))

    workflow_configs = [
        WorkflowConfig(workflow_name="WORKFLOW_1"),
        WorkflowConfig(
            workflow_name="WORKFLOW_2",
            parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
            date_stencil=DateStencil([-1, 0]),
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
        parametrised_workflows = get_parametrised_workflows.run(
            workflow_configs=workflow_configs,
            lists_of_dates=lists_of_dates,
            workflow_storage=workflow_storage,
        )

    assert len(parametrised_workflows) == 5
    for i, refdate in enumerate(lists_of_dates[0]):
        assert parametrised_workflows[i][0].name == "WORKFLOW_1"
        assert parametrised_workflows[i][1] == {
            "reference_date": refdate,
            "date_ranges": [(refdate, refdate.add(days=1))],
        }
    for i in [3, 4]:
        assert parametrised_workflows[i][0].name == "WORKFLOW_2"
        assert parametrised_workflows[i][1] == {
            "DUMMY_PARAM": "DUMMY_VALUE",
            "reference_date": pendulum.date(2016, 1, i + 1),
            "date_ranges": [
                (pendulum.date(2016, 1, i), pendulum.date(2016, 1, i + 1)),
                (pendulum.date(2016, 1, i + 1), pendulum.date(2016, 1, i + 2)),
            ],
        }


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
    get_session_mock = Mock()
    get_most_recent_state_mock = Mock(return_value=state)
    monkeypatch.setattr("autoflow.utils.get_session", get_session_mock)
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
        session=get_session_mock.return_value,
    )
    assert task_state.is_successful()
    assert is_skipped == task_state.is_skipped()


def test_skip_if_already_run_unrecognised_state(monkeypatch, test_logger):
    """
    Test that skip_if_already_run raises a ValueError if get_most_recent_state
    returns an unrecognised state.
    """
    monkeypatch.setattr("autoflow.utils.get_session", Mock())
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


def test_record_workflow_run_state(monkeypatch, test_logger):
    """
    Test that the record_workflow_run_state task calls WorkflowRuns.set_state with the correct arguments.
    """
    get_session_mock = Mock()
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.utils.get_session", get_session_mock)
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
    set_state_mock.assert_called_once_with(
        workflow_name="DUMMY_FLOW",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        state=RunState.success,
        session=get_session_mock.return_value,
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


def test_available_dates_sensor(monkeypatch, postgres_test_db, tmpdir):
    """
    Test that the available_dates_sensor flow runs the specified workflows with
    the correct parameters, and does not run successful workflow runs more than
    once for the same date.
    """
    # Mock flowclient
    flowclient_available_dates = {
        "cdr_type_1": ["2016-01-01", "2016-01-02", "2016-01-03", "2016-01-04"],
        "cdr_type_2": ["2016-01-04", "2016-01-05", "2016-01-06", "2016-01-07"],
        "cdr_type_3": ["2016-01-08"],
    }
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )
    monkeypatch.setattr("flowclient.connect", Mock())
    monkeypatch.setenv("FLOWAPI_TOKEN", "DUMMY_TOKEN")

    # Mock workflows
    workflow_1 = Mock()
    workflow_1.name = "WORKFLOW_1"
    workflow_1.run.return_value = Success()
    workflow_2 = Mock()
    workflow_2.name = "WORKFLOW_2"
    workflow_2.run.return_value = Success()
    workflow_storage = Local(tmpdir)
    workflow_storage.add_flow(workflow_1)
    workflow_storage.add_flow(workflow_2)

    workflow_configs = [
        WorkflowConfig(
            workflow_name="WORKFLOW_1",
            parameters={"DUMMY_PARAM_1": "DUMMY_VALUE_1"},
            earliest_date=pendulum.date(2016, 1, 4),
        ),
        WorkflowConfig(
            workflow_name="WORKFLOW_2",
            parameters={"DUMMY_PARAM_2": "DUMMY_VALUE_2"},
            date_stencil=DateStencil([[pendulum.date(2016, 1, 3), -1], -1, 0]),
        ),
    ]

    # Run available dates sensor
    with set_temporary_config(
        {"flowapi_url": "DUMMY_URL", "db_uri": postgres_test_db.url()}
    ):
        flow_state = available_dates_sensor.run(
            cdr_types=["cdr_type_1", "cdr_type_2"],
            workflow_configs=workflow_configs,
            workflow_storage=workflow_storage,
        )

    # Check that run was successful and workflows were run with the correct parameters
    assert flow_state.is_successful
    workflow_1.run.assert_has_calls(
        [
            call(
                parameters=dict(
                    reference_date=d,
                    date_ranges=[(d, d.add(days=1))],
                    DUMMY_PARAM_1="DUMMY_VALUE_1",
                ),
                run_on_schedule=False,
            )
            for d in pendulum.period(
                pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 7)
            )
        ]
    )
    workflow_2.run.assert_has_calls(
        [
            call(
                parameters=dict(
                    reference_date=d,
                    date_ranges=[
                        (pendulum.date(2016, 1, 3), d.subtract(days=1)),
                        (d.subtract(days=1), d),
                        (d, d.add(days=1)),
                    ],
                    DUMMY_PARAM_2="DUMMY_VALUE_2",
                ),
                run_on_schedule=False,
            )
            for d in pendulum.period(
                pendulum.date(2016, 1, 5), pendulum.date(2016, 1, 7)
            )
        ]
    )

    # Reset workflow mocks
    workflow_1.reset_mock()
    workflow_2.reset_mock()

    # Add more available dates
    flowclient_available_dates = {
        "cdr_type_1": ["2016-01-01", "2016-01-02", "2016-01-03", "2016-01-04"],
        "cdr_type_2": [
            "2016-01-04",
            "2016-01-05",
            "2016-01-06",
            "2016-01-07",
            "2016-01-08",
        ],
        "cdr_type_3": ["2016-01-08"],
    }
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )

    # Run available dates sensor again
    with set_temporary_config(
        {"flowapi_url": "DUMMY_URL", "db_uri": postgres_test_db.url()}
    ):
        flow_state = available_dates_sensor.run(
            cdr_types=["cdr_type_1", "cdr_type_2"],
            workflow_configs=workflow_configs,
            workflow_storage=workflow_storage,
        )

    # Check that workflows only ran for the new date
    workflow_1.run.assert_called_once_with(
        parameters=dict(
            reference_date=pendulum.date(2016, 1, 8),
            date_ranges=[(pendulum.date(2016, 1, 8), pendulum.date(2016, 1, 9))],
            DUMMY_PARAM_1="DUMMY_VALUE_1",
        ),
        run_on_schedule=False,
    )
    workflow_2.run.assert_called_once_with(
        parameters=dict(
            reference_date=pendulum.date(2016, 1, 8),
            date_ranges=[
                (pendulum.date(2016, 1, 3), pendulum.date(2016, 1, 7)),
                (pendulum.date(2016, 1, 7), pendulum.date(2016, 1, 8)),
                (pendulum.date(2016, 1, 8), pendulum.date(2016, 1, 9)),
            ],
            DUMMY_PARAM_2="DUMMY_VALUE_2",
        ),
        run_on_schedule=False,
    )


def test_available_dates_sensor_retries(monkeypatch, postgres_test_db, tmpdir):
    """
    Test that the available_dates_sensor flow re-runs workflows that failed on
    the previous attempt, and does not re-run them again once they have succeeded.
    """
    # Mock flowclient
    flowclient_available_dates = {
        "dummy_cdr_type": ["2016-01-01", "2016-01-02", "2016-01-03"]
    }
    monkeypatch.setattr(
        "flowclient.get_available_dates", lambda connection: flowclient_available_dates
    )
    monkeypatch.setattr("flowclient.connect", Mock())
    monkeypatch.setenv("FLOWAPI_TOKEN", "DUMMY_TOKEN")

    # Mock workflows
    dummy_workflow = Mock()
    dummy_workflow.name = "DUMMY_WORKFLOW"
    dummy_workflow.run.side_effect = [Failed(), Success(), Success()]
    workflow_storage = Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    workflow_configs = [WorkflowConfig(workflow_name="DUMMY_WORKFLOW")]

    # Run available dates sensor
    with set_temporary_config(
        {"flowapi_url": "DUMMY_URL", "db_uri": postgres_test_db.url()}
    ):
        flow_state = available_dates_sensor.run(
            workflow_configs=workflow_configs, workflow_storage=workflow_storage
        )

    # Check that sensor flow ended in a 'failed' state, and dummy_workflow.run() was called 3 times
    assert flow_state.is_failed
    dummy_workflow.run.assert_has_calls(
        [
            call(
                parameters=dict(reference_date=d, date_ranges=[(d, d.add(days=1))]),
                run_on_schedule=False,
            )
            for d in pendulum.period(
                pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 3)
            )
        ]
    )

    # Reset workflow mock
    dummy_workflow.reset_mock()
    dummy_workflow.run.side_effect = None
    dummy_workflow.run.return_value = Success()

    # Run available dates sensor again
    with set_temporary_config(
        {"flowapi_url": "DUMMY_URL", "db_uri": postgres_test_db.url()}
    ):
        flow_state = available_dates_sensor.run(
            workflow_configs=workflow_configs, workflow_storage=workflow_storage
        )

    # Check that sensor flow was successful, and dummy_workflow only re-ran for the date for which it previously failed
    assert flow_state.is_successful
    dummy_workflow.run.assert_called_once_with(
        parameters=dict(
            reference_date=pendulum.date(2016, 1, 1),
            date_ranges=[(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 2))],
        ),
        run_on_schedule=False,
    )

    # Reset workflow mock again
    dummy_workflow.reset_mock()

    # Run available dates sensor once more
    with set_temporary_config(
        {"flowapi_url": "DUMMY_URL", "db_uri": postgres_test_db.url()}
    ):
        flow_state = available_dates_sensor.run(
            workflow_configs=workflow_configs, workflow_storage=workflow_storage
        )

    # Check that dummy_workflow did not run again, now that it has run successfully
    assert flow_state.is_successful
    dummy_workflow.run.assert_not_called()
