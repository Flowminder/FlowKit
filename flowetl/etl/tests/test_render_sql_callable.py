# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from unittest.mock import patch, MagicMock
from pathlib import Path

from airflow import DAG
from airflow.operators import BaseOperator

from etl.production_task_callables import render_and_run_sql__callable


class MockDagRun:
    def __init__(self, conf):
        self.conf = conf


def test_render_and_run_sql_callable(tmpdir):
    """
    Test that the render sql callable is able to construct the
    correct sql from a template in the correct location and issues
    this sql to the db_hook run command.
    """

    # Make temp sql template in "config/etl/voice/init.sql" where
    # config is a temp dir

    config_dir = tmpdir.mkdir("config")
    etl_dir = config_dir.mkdir("etl")
    voice_dir = etl_dir.mkdir("voice")

    template_name = "init"
    init_template_path = voice_dir.join(f"{template_name}.sql")
    init_template = "select {{number}}"
    init_template_path.write(init_template)

    # Mocks so we can use the templating feature of the operator this
    # callable runs in and a mock of the db_hook which will run the sql
    mock_dag = DAG(dag_id="testing", default_args={"start_date": "2016-01-01"})
    mock_task_op = BaseOperator(task_id="testing", dag=mock_dag)
    mock_pghook = MagicMock()

    # Mock of the config passed to the dag_run
    conf = {"number": 23, "template_path": Path("etl/voice")}
    mock_dag_run = MockDagRun(conf=conf)

    render_and_run_sql__callable(
        dag_run=mock_dag_run,
        db_hook=mock_pghook,
        task=mock_task_op,
        config_path=Path(config_dir),
        template_name=template_name,
    )

    _, _, kwargs = mock_pghook.mock_calls[0]

    # assert that the db_hook was called with correct sql
    assert kwargs == {"sql": f"select {conf['number']}"}
