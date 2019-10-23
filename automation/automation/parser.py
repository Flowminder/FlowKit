# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import config
from pathlib import Path
from typing import List, Dict, Tuple, Any
import yaml

from .workflows import make_workflow


def load_and_validate_workflows_yaml(filename: str) -> List[Dict[str, Any]]:
    """
    Load a yaml file that defines workflows, and validate its contents.

    Parameters
    ----------
    filename : str
        Name of yaml input file

    Returns
    -------
    list of dict
        List of dictionaries defining workflows
    """
    # TODO: This would be neater and more robust as a marshmallow schema
    with open(filename, "r") as f:
        workflows_spec = yaml.safe_load(f)

    if not isinstance(workflows_spec, list):
        raise TypeError(
            "Workflows yaml file does not contain a sequence of workflow specifications."
        )
    workflow_names = []
    for workflow_spec in workflows_spec:
        if not isinstance(workflow_spec, dict):
            raise TypeError("Invalid workflow specification: not a mapping.")
        expected_keys = {"name", "kind", "schedule", "notebooks", "parameters"}
        missing_keys = expected_keys.difference(workflow_spec.keys())
        if missing_keys:
            raise KeyError(
                f"Workflow specification parameters {missing_keys} were not provided."
            )
        unexpected_keys = set(workflow_spec.keys()).difference(expected_keys)
        if unexpected_keys:
            raise KeyError(
                f"Received unexpected workflow specification parameters {unexpected_keys}."
            )
        # Note: validity of name, kind and schedule are checked when building the workflow
        if workflow_spec["name"] in workflow_names:
            raise ValueError(f"Duplicate workflow name: '{workflow_spec['name']}'.")
        else:
            workflow_names.append(workflow_spec["name"])
        if not isinstance(workflow_spec["notebooks"], list):
            raise TypeError(
                "Invalid workflow specification: 'notebooks' is not a sequence of notebook task specifications."
            )
        notebook_labels = []
        for notebook in workflow_spec["notebooks"]:
            required_keys = {"label", "filename", "parameters"}
            missing_keys = required_keys.difference(notebook.keys())
            if missing_keys:
                raise KeyError(
                    f"Notebook task specification missing required keys {missing_keys}."
                )
            unexpected_keys = (
                set(notebook.keys()).difference(required_keys).difference({"output"})
            )
            if unexpected_keys:
                raise KeyError(
                    f"Notebook task specification contains unexpected keys {unexpected_keys}."
                )
            if notebook["label"] in notebook_labels:
                raise ValueError(f"Duplicate notebook label: '{notebook['label']}'.")
            else:
                notebook_labels.append(notebook["label"])
            if not (Path(config.inputs.inputs_dir) / notebook["filename"]).exists():
                raise ValueError(f"Notebook file '{notebook['filename']}' not found.")
            if not isinstance(notebook["parameters"], dict):
                raise TypeError(
                    "Invalid notebook task specification: 'parameters' is not a mapping."
                )
            # Note: validity of parameter names is checked when building the workflow.
            if "output" in notebook:
                if isinstance(notebook["output"], dict):
                    if "template" not in notebook["output"]:
                        notebook["output"]["template"] = None
                    if (notebook["output"]["template"] is not None) and not (
                        Path(config.inputs.inputs_dir) / notebook["output"]["template"]
                    ).exists():
                        raise ValueError(
                            f"Template '{notebook['output']['template']}' not found."
                        )
                    unexpected_keys = set(notebook["output"].keys()).difference(
                        {"template"}
                    )
                    if unexpected_keys:
                        raise KeyError(
                            f"Unexpected keys in notebook output block: {unexpected_keys}."
                        )
                elif notebook["output"]:
                    notebook["output"] = dict(template=None)
                else:
                    notebook.pop("output")
        if not isinstance(workflow_spec["parameters"], list):
            raise TypeError(
                "Invalid workflow specification: 'parameters' is not a list of parameter mappings."
            )
        for params in workflow_spec["parameters"]:
            # Note: validity of the provided parameters cannot be checked until the workflow has been constructed.
            if not isinstance(params, dict):
                raise TypeError(
                    "Invalid workflow specification: 'parameters' is not a list of parameter mappings."
                )

    return workflows_spec


def parse_workflows_yaml(
    filename: str
) -> Tuple[List["prefect.Flow"], Dict[str, List[Dict[str, Any]]]]:
    """
    Construct workflows defined in an input file.

    Parameters
    ----------
    filename : str
        Name of yaml input file
    
    Returns
    -------
    workflows : list of Flow
        List of prefect workflows
    run_parameters : dict
        mapping from workflow names to a list of dicts of parameters for which the workflow should be run
    """
    workflow_specs = load_and_validate_workflows_yaml(filename)

    workflows = []
    run_parameters = {}
    for workflow_spec in workflow_specs:
        parameters = workflow_spec.pop("parameters")
        workflow = make_workflow(**workflow_spec)
        workflows.append(workflow)
        # TODO: Store workflow in Local storage, instead of returning in a list?
        for params_dict in parameters:
            param_names = set(params_dict.keys())
            workflow_param_names = {p.name for p in workflow.parameters()}
            required_param_names = {p.name for p in workflow.parameters() if p.required}
            missing_params = required_param_names.difference(param_names)
            if missing_params:
                raise KeyError(
                    f"Missing required parameters {missing_params} for workflow '{workflow.name}'."
                )
            unexpected_params = param_names.difference(workflow_param_names)
            if unexpected_params:
                raise KeyError(
                    f"Unexpected parameters provided for workflow '{workflow.name}': {unexpected_params}."
                )
            # TODO: For date-triggered workflows, we should validate the values for parameters 'cdr_types', 'earliest_date' and 'date_stencil'
        run_parameters[workflow_spec["name"]] = parameters

    return workflows, run_parameters
