# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import datetime
from textwrap import dedent

from prefect import storage
from prefect.schedules import Schedule

from autoflow.date_stencil import DateStencil
from autoflow.parser import parse_workflows_yaml
from autoflow.sensor import WorkflowConfig


def test_parse_workflows_yaml(tmp_path):
    """
    Test that parse_workflows_yaml correctly parses an example input file.
    """
    (tmp_path / "notebook1.ipynb").touch()
    (tmp_path / "notebook2.ipynb").touch()
    (tmp_path / "notebook3.ipynb").touch()
    (tmp_path / "custom_template.tpl").touch()
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            workflows:
              - name: workflow1
                notebooks:
                  notebook1:
                    filename: notebook1.ipynb
                    parameters:
                      url: flowapi_url
                      date: reference_date
                      extra: dummy_param
                  notebook2:
                    filename: notebook2.ipynb
                    parameters:
                      ranges: date_ranges
                      other: notebook1
                    output:
                      format: pdf
                      template: custom_template.tpl
              - name: workflow2
                notebooks:
                  the_notebook:
                    filename: notebook3.ipynb
                    output:
                      format: pdf
            
            available_dates_sensor:
              schedule: "0 0 * * *"
              cdr_types:
                - calls
                - sms
              workflows:
                - workflow_name: workflow1
                  parameters:
                    dummy_param: 123
                  earliest_date: 2016-01-01
                  date_stencil: [[2016-01-01, 2016-01-03], -1, 0]
                - workflow_name: workflow2
            """
        )
    )
    workflow_storage, sensor_config = parse_workflows_yaml(
        filename="dummy_input.yml", inputs_dir=str(tmp_path)
    )
    assert isinstance(workflow_storage, storage.Storage)
    assert "workflow1" in workflow_storage
    assert "workflow2" in workflow_storage
    assert isinstance(sensor_config["schedule"], Schedule)
    assert sensor_config["cdr_types"] == ["calls", "sms"]
    assert sensor_config["workflows"] == [
        WorkflowConfig(
            workflow_name="workflow1",
            parameters={"dummy_param": 123},
            earliest_date=datetime.date(2016, 1, 1),
            date_stencil=DateStencil(
                [[datetime.date(2016, 1, 1), datetime.date(2016, 1, 3)], -1, 0]
            ),
        ),
        WorkflowConfig(workflow_name="workflow2"),
    ]


def test_parse_workflows_yaml_missing_workflows(tmp_path):
    """
    Test that parse_workflows_yaml raises a ValueError if the input file
    doesn't have a 'workflows' key.
    """
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            available_dates_sensor:
              schedule: "0 0 * * *"
              workflows:
                - workflow_name: workflow1
            """
        )
    )
    with pytest.raises(
        ValueError, match="Input file does not have a 'workflows' section."
    ):
        workflow_storage, sensor_config = parse_workflows_yaml(
            filename="dummy_input.yml", inputs_dir=str(tmp_path)
        )


def test_parse_workflows_yaml_missing_available_dates_sensor(tmp_path):
    """
    Test that parse_workflows_yaml raises a ValueError if the input file
    doesn't have a 'workflows' key.
    """
    (tmp_path / "notebook1.ipynb").touch()
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            workflows:
              - name: workflow1
                notebooks:
                  notebook1:
                    filename: notebook1.ipynb
            """
        )
    )
    with pytest.raises(
        ValueError,
        match="Input file does not have an 'available_dates_sensor' section.",
    ):
        workflow_storage, sensor_config = parse_workflows_yaml(
            filename="dummy_input.yml", inputs_dir=str(tmp_path)
        )
