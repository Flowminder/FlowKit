# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from collections import OrderedDict
from unittest.mock import call, Mock

import pendulum
import prefect
from prefect.utilities.configuration import set_temporary_config

from autoflow.workflows import (
    convert_notebook_to_pdf,
    get_flowapi_url,
    get_tag,
    make_notebooks_workflow,
    papermill_execute_notebook,
)
from autoflow.utils import get_params_hash


def test_get_flowapi_url():
    """
    Test that the get_flowapi_url task returns the FlowAPI URL set in the prefect config.
    """
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}):
        flowapi_url = get_flowapi_url.run()
    assert flowapi_url == "DUMMY_URL"


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
    assert tag == f"DUMMY-FLOW-NAME_2016-01-01_{get_params_hash(dummy_params)}"


def test_papermill_execute_notebook(monkeypatch, test_logger):
    """
    Test that the papermill_execute_notebook task calls papermill.execute_notebook
    with the correct arguments, and returns the output filename.
    """
    execute_notebook_mock = Mock()
    get_output_filename_mock = Mock(return_value="DUMMY_OUTPUT_FILENAME")
    monkeypatch.setattr(
        "autoflow.workflows.make_json_serialisable",
        lambda x: {k: f"SAFE_{v}" for k, v in x.items()},
    )
    monkeypatch.setattr(
        "autoflow.workflows.get_output_filename", get_output_filename_mock
    )
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
        "autoflow.workflows.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.workflows.asciidoc_to_pdf", asciidoc_to_pdf_mock)

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
        "autoflow.workflows.notebook_to_asciidoc",
        Mock(return_value=("DUMMY_BODY", "DUMMY_RESOURCES")),
    )
    monkeypatch.setattr("autoflow.workflows.asciidoc_to_pdf", asciidoc_to_pdf_mock)

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
        "autoflow.workflows.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.workflows.asciidoc_to_pdf", Mock())

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
        "autoflow.workflows.notebook_to_asciidoc", notebook_to_asciidoc_mock
    )
    monkeypatch.setattr("autoflow.workflows.asciidoc_to_pdf", Mock())

    with set_temporary_config(
        {"outputs.reports_dir": "DUMMY_REPORTS_DIR"}
    ), prefect.context(logger=test_logger):
        prefect.config.pop("asciidoc_template_path", None)
        output_path = convert_notebook_to_pdf.run(
            notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb"
        )

    notebook_to_asciidoc_mock.assert_called_once_with(
        "DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb", None
    )


def test_make_notebooks_workflow(monkeypatch):
    """
    Test that make_notebooks_workflow returns a workflow with the correct name
    and parameters, and the workflow can run successfully and executes the
    correct tasks.
    """
    # Patch tasks
    monkeypatch.setattr(
        "autoflow.workflows.get_tag.run", Mock(return_value="DUMMY_TAG")
    )
    execute_notebook_mock = Mock(
        side_effect=["DUMMY_OUTPUT_1.ipynb", "DUMMY_OUTPUT_2.ipynb"]
    )
    monkeypatch.setattr(
        "autoflow.workflows.papermill_execute_notebook.run", execute_notebook_mock
    )
    convert_to_pdf_mock = Mock(return_value="DUMMY_REPORT.pdf")
    monkeypatch.setattr(
        "autoflow.workflows.convert_notebook_to_pdf.run", convert_to_pdf_mock
    )

    # Create workflow
    notebooks = OrderedDict(
        [
            (
                "notebook1",
                dict(
                    filename="DUMMY_NOTEBOOK1.ipynb",
                    parameters={
                        "param1": "reference_date",
                        "param2": "flowapi_url",
                        "param3": "DUMMY_PARAM",
                    },
                ),
            ),
            (
                "notebook2",
                dict(
                    filename="DUMMY_NOTEBOOK2.ipynb",
                    parameters={"param1": "date_ranges", "param2": "notebook1"},
                    output={"format": "pdf", "template": None},
                ),
            ),
        ]
    )
    dummy_workflow = make_notebooks_workflow(name="DUMMY_WORKFLOW", notebooks=notebooks)

    # Check workflow has correct name and parameters
    assert dummy_workflow.name == "DUMMY_WORKFLOW"
    workflow_parameter_names = {p.name for p in dummy_workflow.parameters()}
    assert workflow_parameter_names == {"reference_date", "date_ranges", "DUMMY_PARAM"}

    # Run workflow
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}):
        flow_state = dummy_workflow.run(
            reference_date=pendulum.date(2016, 1, 1),
            date_ranges=[(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1))],
            DUMMY_PARAM="DUMMY_VALUE",
        )

    # Check workflow succeeded and ran the correct tasks
    assert flow_state.is_successful
    execute_notebook_mock.assert_has_calls(
        [
            call(
                input_filename="DUMMY_NOTEBOOK1.ipynb",
                output_tag="DUMMY_TAG",
                parameters={
                    "param1": pendulum.date(2016, 1, 1),
                    "param2": "DUMMY_URL",
                    "param3": "DUMMY_VALUE",
                },
            ),
            call(
                input_filename="DUMMY_NOTEBOOK2.ipynb",
                output_tag="DUMMY_TAG",
                parameters={
                    "param1": [(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1))],
                    "param2": "DUMMY_OUTPUT_1.ipynb",
                },
            ),
        ]
    )
    convert_to_pdf_mock.assert_called_once_with(
        notebook_path="DUMMY_OUTPUT_2.ipynb", asciidoc_template=None
    )


def test_make_notebooks_workflow_default_parameters():
    """
    Test that a workflow returned by make_notebooks_workflow has parameters
    'reference_date' and 'date_ranges', even if these parameters are not used
    by the notebooks.
    """
    notebooks = OrderedDict([("notebook1", dict(filename="DUMMY_NOTEBOOK1.ipynb"))])
    dummy_workflow = make_notebooks_workflow(name="DUMMY_WORKFLOW", notebooks=notebooks)
    workflow_parameter_names = {p.name for p in dummy_workflow.parameters()}
    assert workflow_parameter_names == {"reference_date", "date_ranges"}


def test_notebooks_workflow_fails(monkeypatch):
    """
    Test that a workflow returned by make_notebooks_workflow ends in a failed state
    if an exception is raised while executing a notebook.
    """
    # Patch tasks
    monkeypatch.setattr(
        "autoflow.workflows.get_tag.run", Mock(return_value="DUMMY_TAG")
    )
    execute_notebook_mock = Mock(side_effect=Exception("This task failed"))
    monkeypatch.setattr(
        "autoflow.workflows.papermill_execute_notebook.run", execute_notebook_mock
    )

    # Create workflow
    notebooks = OrderedDict([("notebook1", dict(filename="DUMMY_NOTEBOOK1.ipynb"))])
    dummy_workflow = make_notebooks_workflow(name="DUMMY_WORKFLOW", notebooks=notebooks)

    # Run workflow
    with set_temporary_config({"flowapi_url": "DUMMY_URL"}):
        flow_state = dummy_workflow.run(
            reference_date=pendulum.date(2016, 1, 1),
            date_ranges=[(pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1))],
        )

    # Check workflow ran teh notebook execution task, and ended in a failed state
    assert flow_state.is_failed
    execute_notebook_mock.assert_called_once_with(
        input_filename="DUMMY_NOTEBOOK1.ipynb", output_tag="DUMMY_TAG", parameters={}
    )
