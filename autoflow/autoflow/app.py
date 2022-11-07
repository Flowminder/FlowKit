# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the 'main' function, whcih runs when __main__.py is executed.
"""

import logging
import os
import time
from pathlib import Path

from get_secret_or_env_var import getenv

from autoflow.model import init_db
from autoflow.parser import parse_workflows_yaml
from autoflow.sensor import available_dates_sensor


def main(run_on_schedule: bool = True):
    """
    Main function. Creates output directories, initialises the database, parses
    a workflows definition file to define workflows and configure the available
    dates sensor, and runs the available dates sensor.

    Parameters
    ----------
    run_on_schedule : bool, default True
        Set run_on_schedule=False to run the sensor only once, ignoring the schedule.
        (useful for testing)
    """
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
    inputs_dir = os.environ["AUTOFLOW_INPUTS_DIR"]
    workflow_storage_dir = str(outputs_path / ".prefect/flows")
    logger.info(f"Creating workflows defined in '{Path(inputs_dir)/'workflows.yml'}'.")
    workflow_storage, sensor_config = parse_workflows_yaml(
        "workflows.yml", inputs_dir, workflow_storage_dir
    )

    # Run available dates sensor
    logger.info("Running available dates sensor.")
    available_dates_sensor.schedule = sensor_config["schedule"]
    available_dates_sensor.run(
        workflow_configs=sensor_config["workflows"],
        cdr_types=sensor_config["cdr_types"],
        workflow_storage=workflow_storage,
        run_on_schedule=run_on_schedule,
    )
