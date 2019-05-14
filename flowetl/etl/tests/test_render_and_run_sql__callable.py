# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from unittest.mock import patch, Mock
from pathlib import Path

from airflow import DAG
from airflow.operators import BaseOperator

from etl.production_task_callables import render_and_run_sql__callable


class FakeDagRun:
    def __init__(self, conf):
        self.conf = conf


def test_render_and_run_sql__callable(tmpdir, create_fake_dag_run):
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
    fake_dag = DAG(dag_id="testing", default_args={"start_date": "2016-01-01"})
    fake_task_op = BaseOperator(task_id="testing", dag=fake_dag)
    mock_pghook = Mock()

    # Mock of the config passed to the dag_run
    conf = {"number": 23, "template_path": Path("etl/voice")}
    fake_dag_run = create_fake_dag_run(conf=conf)

    # make sure the mock has not been called some other way
    assert mock_pghook.mock_calls == []

    render_and_run_sql__callable(
        dag_run=fake_dag_run,
        db_hook=mock_pghook,
        task=fake_task_op,
        config_path=Path(config_dir),
        template_name=template_name,
    )

    assert len(mock_pghook.mock_calls) == 1

    # assert that the db_hook was called with correct sql
    _, _, kwargs = mock_pghook.mock_calls[0]
    assert kwargs == {"sql": f"select {conf['number']}"}
