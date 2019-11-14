# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Main script, that will be executed when running `python -m autoflow`.
Creates output directories, initialises the database, parses a workflows definition file to define workflows, and runs the workflows.
"""

import logging
import os
import time
from pathlib import Path
from get_secret_or_env_var import getenv

from .model import init_db
from .parser import parse_workflows_yaml
from .workflows import run_workflows


def main():
    # Initialise logger
    # TODO: Use structlog (not sure whether it will be possible for the prefect logger)
    log_level = os.environ["AUTOFLOW_LOG_LEVEL"]
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s | %(message)s"
    )  # Match prefect format for now
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)
    logger.info(f"Log level for logger '{__name__}' set to '{log_level}'.")

    # Make output directories
    outputs_path = Path(os.environ["AUTOFLOW_OUTPUTS_DIR"])
    logger.info(
        f"Creating output directories '{outputs_path/'notebooks'}' and '{outputs_path/'reports'}'."
    )
    (outputs_path / "notebooks").mkdir(exist_ok=True)
    (outputs_path / "reports").mkdir(exist_ok=True)

    # Init DB
    # Note: AUTOFLOW_DB_URI must be an env var so that it can be used in prefect.config, so we read it using os.environ.
    # AUTOFLOW_DB_PASSWORD can (and should) be a docker secret, so we read it using get_secret_or_env_var.
    db_uri = os.environ["AUTOFLOW_DB_URI"]
    logger.info(f"Initialising database '{db_uri}'.")
    init_db(db_uri.format(getenv("AUTOFLOW_DB_PASSWORD", "")))

    # Create workflows according to workflow definition file
    inputs_dir = Path(os.environ["AUTOFLOW_INPUTS_DIR"])
    logger.info(f"Creating workflows defined in '{inputs_dir/'workflows.yml'}'.")
    workflows, parameters = parse_workflows_yaml("workflows.yml", inputs_dir)

    # Run workflows
    logger.info("Running workflows.")
    run_workflows(workflows, parameters)
    # TODO: run the available dates sensor, instead of running the workflows directly
    # schedule : str
    #     Cron string describing the schedule on which the sensor will check for new data.
    # workflow_configs : list of WorkflowConfig
    #     List of workflows that the dates sensor should trigger.
    # cdr_types: list of str, optional
    #     A list of CDR types for which available dates will be checked (default is all available CDR types).
    # available_dates_sensor.schedule = CronSchedule(schedule)
    # available_dates_sensor.run(workflow_configs=workflow_configs, workflow_storage=workflow_storage, cdr_types=cdr_types)


if __name__ == "__main__":
    main()
