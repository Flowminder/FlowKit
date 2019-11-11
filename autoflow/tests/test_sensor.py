# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import prefect
import pendulum
from prefect.utilities.configuration import set_temporary_config
from unittest.mock import Mock

from autoflow.sensor import (
    filter_dates,
    get_available_dates,
    record_workflow_run_state,
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


# TODO: Adapt these tests for skip_if_already_run
# def test_filter_dates_by_previous_runs(monkeypatch, test_logger):
#     """
#     Test that the filter_dates_by_previous_runs removes dates for which WorkFlowRuns.can_process returns False.
#     """
#     dates = [pendulum.date(2016, 1, d) for d in [1, 2, 3]]
#
#     def dummy_can_process(workflow_name, workflow_params, reference_date, session):
#         if reference_date in [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)]:
#             return True
#         else:
#             return False
#
#     session_mock = Mock()
#     get_session_mock = Mock(return_value=session_mock)
#     can_process_mock = Mock(side_effect=dummy_can_process)
#     monkeypatch.setattr("autoflow.tasks.get_session", get_session_mock)
#     monkeypatch.setattr("autoflow.tasks.WorkflowRuns.can_process", can_process_mock)
#
#     with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
#         flow_name="DUMMY_WORFLOW_NAME",
#         parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
#         logger=test_logger,
#     ):
#         filtered_dates = filter_dates_by_previous_runs.run(dates)
#
#     get_session_mock.assert_called_once_with("DUMMY_DB_URI")
#     can_process_mock.assert_has_calls(
#         [
#             call(
#                 workflow_name="DUMMY_WORFLOW_NAME",
#                 workflow_params={"DUMMY_PARAM": "DUMMY_VALUE"},
#                 reference_date=d,
#                 session=session_mock,
#             )
#             for d in dates
#         ]
#     )
#     session_mock.close.assert_called_once()
#     assert filtered_dates == [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)]
#
#
# @pytest.mark.parametrize(
#     "state,expected", [("in_process", False), ("done", False), ("failed", True)]
# )
# def test_can_process(session, state, expected):
#     """
#     Test that can_process returns True for 'failed' runs, False for 'in_process' or 'done' runs.
#     """
#     workflow_run_data = dict(
#         workflow_name="DUMMY_WORKFLOW_NAME",
#         workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
#         reference_date=pendulum.parse("2016-01-01", exact=True),
#     )
#     WorkflowRuns.set_state(
#         **workflow_run_data,
#         scheduled_start_time=pendulum.parse("2016-01-02T13:00:00Z"),
#         state=state,
#         session=session,
#     )
#
#     res = WorkflowRuns.can_process(**workflow_run_data, session=session)
#     assert res == expected
#
#
# def test_can_process_new(session):
#     """
#     Test that 'can_process' returns True for a new workflow run.
#     """
#     workflow_run_data = dict(
#         workflow_name="DUMMY_WORKFLOW_NAME",
#         workflow_params={"DUMMY_PARAM_NAME": "DUMMY_PARAM_VALUE"},
#         reference_date=pendulum.parse("2016-01-01", exact=True),
#     )
#     res = WorkflowRuns.can_process(**workflow_run_data, session=session)
#     assert res


# TODO: Incorporate these tests into tests for add_dates_to_parameters
# def test_get_date_ranges(monkeypatch):
#     """
#     Test that the get_date_ranges task returns the result of stencil_to_date_pairs.
#     """
#     stencil_to_date_pairs_mock = Mock(return_value="DUMMY_DATE_PAIRS")
#     monkeypatch.setattr(
#         "autoflow.tasks.stencil_to_date_pairs", stencil_to_date_pairs_mock
#     )
#     reference_date = pendulum.date(2016, 1, 1)
#     date_stencil = [-1, 0]
#     date_ranges = get_date_ranges.run(
#         reference_date=reference_date, date_stencil=date_stencil
#     )
#     assert date_ranges == "DUMMY_DATE_PAIRS"
#     stencil_to_date_pairs_mock.assert_called_once_with(
#         stencil=date_stencil, reference_date=reference_date
#     )
#
# def test_get_date_ranges_default():
#     """
#     Test that if no stencil is provided, the get_date_ranges task returns a single date range containing only the reference date.
#     """
#     reference_date = pendulum.date(2016, 1, 1)
#     date_ranges = get_date_ranges.run(reference_date=reference_date)
#     assert date_ranges == [(reference_date, reference_date)]

# TODO: Add tests for add_dates_to_parameters


def test_record_workflow_run_state(monkeypatch, test_logger):
    """
    Test that the record_workflow_run_state task calls WorkflowRuns.set_state with the correct arguments.
    """
    session_mock = Mock()
    get_session_mock = Mock(return_value=session_mock)
    set_state_mock = Mock()
    monkeypatch.setattr("autoflow.sensor.get_session", get_session_mock)
    monkeypatch.setattr("autoflow.sensor.WorkflowRuns.set_state", set_state_mock)
    dummy_parameterised_workflow = (
        prefect.Flow(name="DUMMY_FLOW"),
        {"DUMMY_PARAM": "DUMMY_VALUE"},
    )
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        logger=test_logger
    ):
        record_workflow_run_state.run(
            parameterised_workflow=dummy_parameterised_workflow, state="DUMMY_STATE"
        )
    get_session_mock.assert_called_once_with("DUMMY_DB_URI")
    session_mock.close.assert_called_once()
    set_state_mock.assert_called_once_with(
        workflow_name="DUMMY_FLOW",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        state="DUMMY_STATE",
        session=session_mock,
    )


# TODO: Add tests for run_workflow, available_dates_sensor, and run_available_dates_sensor

# TODO: Maybe use bits of these tests when testing available_dates_sensor
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
