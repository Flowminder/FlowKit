# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import shutil

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from pathlib import Path


def test_autoflow_example(monkeypatch, tmp_path, flowapi_url, universal_access_token):
    """
    Test that the AutoFlow example runs correctly and produces the expected output.
    """
    # Set up files and directories
    config_dir = tmp_path / "config"
    inputs_dir = tmp_path / "inputs"
    outputs_dir = tmp_path / "outputs"
    shutil.copytree(
        Path(__file__).parent.parent.parent.parent / "autoflow" / "config", config_dir
    )
    shutil.copytree(
        Path(__file__).parent.parent.parent.parent / "autoflow" / "examples" / "inputs",
        inputs_dir,
    )
    outputs_dir.mkdir()
    # Database URI
    db_uri = f"sqlite:///{str(tmp_path/'test.db')}"
    # Set environment variables
    monkeypatch.setenv("AUTOFLOW_INPUTS_DIR", str(inputs_dir))
    monkeypatch.setenv("AUTOFLOW_OUTPUTS_DIR", str(outputs_dir))
    monkeypatch.setenv("AUTOFLOW_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("AUTOFLOW_DB_URI", db_uri)
    monkeypatch.setenv("FLOWAPI_URL", flowapi_url)
    monkeypatch.setenv("FLOWAPI_TOKEN", universal_access_token)
    monkeypatch.setenv("PREFECT__USER_CONFIG_PATH", str(config_dir / "config.toml"))
    monkeypatch.setenv(
        "PREFECT__ASCIIDOC_TEMPLATE_PATH", str(config_dir / "asciidoc_extended.tpl")
    )
    # Import autoflow (this must be done after setting the environment variables,
    # otherwise they won't be picked up in the prefect config)
    from autoflow import app, sensor

    # Run AutoFlow
    app.main(run_on_schedule=False)

    # Test that the sensor has the correct schedule
    assert sensor.available_dates_sensor.schedule.clocks[0].cron == "0 0 * * *"
    # Test that the expected files have been created in outputs_dir
    expected_files = [
        "notebooks/run_flows__flows_above_normal_2016-01-0{}_*__*Z.ipynb",
        "notebooks/flows_report__flows_above_normal_2016-01-0{}_*__*Z.ipynb",
        "reports/flows_report__flows_above_normal_2016-01-0{}_*__*Z.pdf",
    ]
    expected_file_counts = [0, 0, 1, 2, 2, 2, 2]
    for file_pattern in expected_files:
        assert [
            len(list(outputs_dir.glob(file_pattern.format(d)))) for d in range(1, 8)
        ] == expected_file_counts
    # Check the content of the database
    Base = automap_base()
    engine = create_engine(db_uri)
    Base.prepare(engine, reflect=True)
    assert "workflow_runs" in Base.metadata.tables
    session = Session(engine)
    assert (
        len(session.query(Base.classes.workflow_runs).filter_by(state="running").all())
        == 9
    )
    assert (
        len(session.query(Base.classes.workflow_runs).filter_by(state="success").all())
        == 9
    )
    assert (
        len(session.query(Base.classes.workflow_runs).filter_by(state="failed").all())
        == 0
    )
    session.close()
