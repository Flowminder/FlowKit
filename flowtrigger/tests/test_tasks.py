# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import prefect
import pendulum
from unittest.mock import Mock
from prefect.utilities.configuration import set_temporary_config

from flowtrigger.tasks import *
from flowtrigger.utils import get_params_hash


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
        "flowtrigger.tasks.stencil_to_date_pairs", stencil_to_date_pairs_mock
    )
    reference_date = pendulum.date(2016, 1, 1)
    date_stencil = [-1, 0]
    date_ranges = get_date_ranges.run(
        reference_date=reference_date, date_stencil=date_stencil
    )
    assert date_ranges == "DUMMY_DATE_PAIRS"
    assert stencil_to_date_pairs_mock.called_once_with(
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
    assert connect_mock.called_once_with(url="DUMMY_URL", token="DUMMY_TOKEN")
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
    monkeypatch.setattr("flowtrigger.tasks.get_session", get_session_mock)
    monkeypatch.setattr("flowtrigger.tasks.WorkflowRuns.can_process", dummy_can_process)
    with set_temporary_config({"db_uri": "DUMMY_DB_URI"}), prefect.context(
        flow_name="DUMMY_WORFLOW_NAME",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        logger=test_logger,
    ):
        filtered_dates = filter_dates_by_previous_runs.run(dates)
    assert get_session_mock.called_once_with("DUMMY_DB_URI")
    assert session_mock.close.called_once()
    assert filtered_dates == [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)]
