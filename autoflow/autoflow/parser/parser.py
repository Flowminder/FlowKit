# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the 'parse_workflows_yaml' function, and marshmallow schemas for parsing
a 'workflows.yml' file to define workflows and configure the available dates sensor.
"""

from pathlib import Path
from typing import Dict, List, Tuple, Union

import yaml

from autoflow.parser.available_dates_sensor_schema import AvailableDatesSensorSchema
from autoflow.parser.workflow_schema import WorkflowSchema


def parse_workflows_yaml(
    filename: str, inputs_dir: str
) -> Tuple[
    "prefect.storage.Storage",
    Dict[
        str,
        Union[
            "prefect.schedules.Schedule",
            List[str],
            List["autoflow.sensor.WorkflowConfig"],
        ],
    ],
]:
    """
    Construct workflows defined in an input file.

    Parameters
    ----------
    filename : str
        Name of yaml input file
    inputs_dir : Path
        Directory in which input files should be found

    Returns
    -------
    workflows : list of Flow
        List of prefect workflows
    run_parameters : dict
        mapping from workflow names to a list of dicts of parameters for which the workflow should be run
    """
    with open(Path(inputs_dir) / filename, "r") as f:
        workflows_yaml = yaml.safe_load(f)

    try:
        workflows_spec = workflows_yaml["workflows"]
    except KeyError:
        raise ValueError("Input file does not have a 'workflows' section.")
    try:
        sensor_spec = workflows_yaml["available_dates_sensor"]
    except KeyError:
        raise ValueError(
            "Input file does not have an 'available_dates_sensor' section."
        )

    workflow_schema = WorkflowSchema(context=dict(inputs_dir=inputs_dir))
    workflow_storage = workflow_schema.load(workflows_spec, many=True)

    sensor_schema = AvailableDatesSensorSchema(
        context=dict(workflow_storage=workflow_storage)
    )
    sensor_config = sensor_schema.load(sensor_spec)

    return workflow_storage, sensor_config
