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


def test_papermill_execute_notebook(monkeypatch, test_logger):
    """
    Test that the papermill_execute_notebook task calls papermill.execute_notebook
    with the correct arguments, and returns the output filename.
    """
    execute_notebook_mock = Mock()
    get_output_filename_mock = Mock(return_value="DUMMY_OUTPUT_FILENAME")
    monkeypatch.setattr(
        "autoflow.tasks.make_json_serialisable",
        lambda x: {k: f"SAFE_{v}" for k, v in x.items()},
    )
    monkeypatch.setattr("autoflow.tasks.get_output_filename", get_output_filename_mock)
    monkeypatch.setattr("papermill.execute_notebook", execute_notebook_mock)

    with set_temporary_config(
        {
            "inputs.inputs_dir": "DUMMY_INPUTS_DIR",
            "outputs.notebooks_dir": "DUMMY_NOTEBOOKS_DIR",
        }
    ), prefect.context(logger=test_logger):
        output_path = papermill_execute_notebook.run(
            input_filename="DUMMY_INPUT_FILENAME",
            output_tag="DUMMY_TAG",
            parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
            dummy_kwarg="DUMMY_KWARG_VALUE",
        )

    assert output_path == "DUMMY_NOTEBOOKS_DIR/DUMMY_OUTPUT_FILENAME"
    get_output_filename_mock.assert_called_once_with(
        input_filename="DUMMY_INPUT_FILENAME", tag="DUMMY_TAG"
    )
    execute_notebook_mock.assert_called_once_with(
        "DUMMY_INPUTS_DIR/DUMMY_INPUT_FILENAME",
        "DUMMY_NOTEBOOKS_DIR/DUMMY_OUTPUT_FILENAME",
        parameters={"DUMMY_PARAM": "SAFE_DUMMY_VALUE"},
        dummy_kwarg="DUMMY_KWARG_VALUE",
    )


def test_convert_notebook_to_pdf(monkeypatch, test_logger):
    """
    Test that the convert_notebook_to_pdf task calls notebook_to_asciidoc
    followed by asciidoc_to_pdf, with the correct arguments.
    """
    notebook_to_asciidoc_mock = Mock(return_value=("DUMMY_BODY", "DUMMY_RESOURCES"))
    asciidoc_to_pdf_mock = Mock()
    monkeypatch.setattr(
        "autoflow.tasks.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.tasks.asciidoc_to_pdf", asciidoc_to_pdf_mock)

    with set_temporary_config(
        {
            "asciidoc_template_path": "DUMMY_TEMPLATE_PATH",
            "outputs.reports_dir": "DUMMY_REPORTS_DIR",
        }
    ), prefect.context(logger=test_logger):
        output_path = convert_notebook_to_pdf.run(
            notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb",
            output_filename="DUMMY_OUTPUT_FILENAME",
        )

    assert output_path == "DUMMY_REPORTS_DIR/DUMMY_OUTPUT_FILENAME"
    notebook_to_asciidoc_mock.assert_called_once_with(
        "DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb", "DUMMY_TEMPLATE_PATH"
    )
    asciidoc_to_pdf_mock.assert_called_once_with(
        "DUMMY_BODY", "DUMMY_RESOURCES", "DUMMY_REPORTS_DIR/DUMMY_OUTPUT_FILENAME"
    )


def test_convert_notebook_to_pdf_default_output_filename(monkeypatch, test_logger):
    """
    Test that if the output filename is not specified, the convert_notebook_to_pdf
    task will use the stem from the input filename, with extension changed to '.pdf'.
    """
    asciidoc_to_pdf_mock = Mock()
    monkeypatch.setattr(
        "autoflow.tasks.notebook_to_asciidoc",
        Mock(return_value=("DUMMY_BODY", "DUMMY_RESOURCES")),
    )
    monkeypatch.setattr("autoflow.tasks.asciidoc_to_pdf", asciidoc_to_pdf_mock)

    with set_temporary_config(
        {
            "asciidoc_template_path": "DUMMY_TEMPLATE_PATH",
            "outputs.reports_dir": "DUMMY_REPORTS_DIR",
        }
    ), prefect.context(logger=test_logger):
        output_path = convert_notebook_to_pdf.run(
            notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb"
        )

    assert output_path == "DUMMY_REPORTS_DIR/DUMMY_FILENAME.pdf"
    asciidoc_to_pdf_mock.assert_called_once_with(
        "DUMMY_BODY", "DUMMY_RESOURCES", "DUMMY_REPORTS_DIR/DUMMY_FILENAME.pdf"
    )


def test_convert_notebook_to_pdf_custom_template(monkeypatch, test_logger):
    """
    Test that if a custom asciidoc template filename is passed to the convert_notebook_to_pdf
    task, that template will be used instead of the default.
    """
    notebook_to_asciidoc_mock = Mock(return_value=("DUMMY_BODY", "DUMMY_RESOURCES"))
    monkeypatch.setattr(
        "autoflow.tasks.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.tasks.asciidoc_to_pdf", Mock())

    with set_temporary_config(
        {
            "asciidoc_template_path": "DEFAULT_TEMPLATE_PATH",
            "inputs.inputs_dir": "DUMMY_INPUTS_DIR",
            "outputs.reports_dir": "DUMMY_REPORTS_DIR",
        }
    ), prefect.context(logger=test_logger):
        output_path = convert_notebook_to_pdf.run(
            notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb",
            asciidoc_template="CUSTOM_TEMPLATE_FILENAME",
        )

    notebook_to_asciidoc_mock.assert_called_once_with(
        "DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb",
        "DUMMY_INPUTS_DIR/CUSTOM_TEMPLATE_FILENAME",
    )


def test_convert_notebook_to_pdf_no_template(monkeypatch, test_logger):
    """
    Test that if no asciidoc template filename is passed to the convert_notebook_to_pdf
    task, and no default template is set in the config, no template will be passed to notebook_to_asciidoc.
    """
    notebook_to_asciidoc_mock = Mock(return_value=("DUMMY_BODY", "DUMMY_RESOURCES"))
    monkeypatch.setattr(
        "autoflow.tasks.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.tasks.asciidoc_to_pdf", Mock())

    with set_temporary_config(
        {"outputs.reports_dir": "DUMMY_REPORTS_DIR"}
    ), prefect.context(logger=test_logger):
        output_path = convert_notebook_to_pdf.run(
            notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb"
        )

    notebook_to_asciidoc_mock.assert_called_once_with(
        "DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb", None
    )
