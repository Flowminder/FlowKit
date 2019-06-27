# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from unittest.mock import Mock
from pathlib import Path
from pendulum import utcnow, parse
from uuid import uuid1

from etl.model import ETLRecord
from etl.etl_utils import CDRType
from etl.config_constant import config
from etl.production_task_callables import trigger__callable


def test_trigger__callable_bad_file_filtered(tmpdir, session, monkeypatch):
    """
    Test that the trigger callable picks up files in files and suitably filters
    them. In this case we have one unseen file and one file that matches no
    pattern. We expect a single call of the trigger_dag_mock for the unseen file.
    """
    files = tmpdir.mkdir("files")

    file1 = files.join("SMS_20160101.csv.gz")
    file1.write("blah")
    file2 = files.join("bad_file.bad")
    file2.write("blah")

    cdr_type_config = config["etl"]
    fake_dag_run = {}
    trigger_dag_mock = Mock()
    now = utcnow()
    uuid = uuid1()
    uuid_sans_underscore = str(uuid).replace("-", "")

    monkeypatch.setattr("etl.production_task_callables.uuid1", lambda: uuid)
    monkeypatch.setattr("etl.production_task_callables.utcnow", lambda: now)
    monkeypatch.setattr("etl.etl_utils.get_session", lambda: session)
    monkeypatch.setattr("etl.production_task_callables.trigger_dag", trigger_dag_mock)

    trigger__callable(
        dag_run=fake_dag_run, cdr_type_config=cdr_type_config, files_path=Path(files)
    )

    assert trigger_dag_mock.call_count == 1

    cdr_type = CDRType("sms")
    cdr_date = parse("2016-01-01")
    expected_conf = {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "file_name": "SMS_20160101.csv.gz",
        "template_path": "etl/sms",
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type}_{str(cdr_date.date()).replace('-','')}",
    }

    trigger_dag_mock.assert_called_with(
        "etl_sms",
        conf=expected_conf,
        execution_date=now,
        run_id=f"SMS_20160101.csv.gz-{uuid}",
        replace_microseconds=False,
    )


def test_trigger__callable_quarantined_file_not_filtered(tmpdir, session, monkeypatch):
    """
    Test that the trigger callable picks up files in files and suitably filters
    them. In this case we have one previously seen and quarantined file.
    We expect a single call of the trigger_dag_mock for the quarantined file.
    """
    files = tmpdir.mkdir("files")

    file1 = files.join("SMS_20160101.csv.gz")
    file1.write("blah")

    # add a some etl records
    file1_data = {
        "cdr_type": "sms",
        "cdr_date": parse("2016-01-01").date(),
        "state": "quarantine",
    }

    ETLRecord.set_state(
        cdr_type=file1_data["cdr_type"],
        cdr_date=file1_data["cdr_date"],
        state=file1_data["state"],
        session=session,
    )

    cdr_type_config = config["etl"]
    fake_dag_run = {}
    trigger_dag_mock = Mock()
    now = utcnow()
    uuid = uuid1()
    uuid_sans_underscore = str(uuid).replace("-", "")

    monkeypatch.setattr("etl.production_task_callables.uuid1", lambda: uuid)
    monkeypatch.setattr("etl.production_task_callables.utcnow", lambda: now)
    monkeypatch.setattr("etl.etl_utils.get_session", lambda: session)
    monkeypatch.setattr("etl.production_task_callables.trigger_dag", trigger_dag_mock)

    trigger__callable(
        dag_run=fake_dag_run, cdr_type_config=cdr_type_config, files_path=Path(files)
    )

    assert trigger_dag_mock.call_count == 1

    cdr_type = CDRType("sms")
    cdr_date = parse("2016-01-01")
    expected_conf = {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "file_name": "SMS_20160101.csv.gz",
        "template_path": "etl/sms",
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type}_{str(cdr_date.date()).replace('-','')}",
    }

    trigger_dag_mock.assert_called_with(
        "etl_sms",
        conf=expected_conf,
        execution_date=now,
        run_id=f"SMS_20160101.csv.gz-{uuid}",
        replace_microseconds=False,
    )


def test_trigger__callable_archive_file_filtered(tmpdir, session, monkeypatch):
    """
    Test that the trigger callable picks up files in files and suitably filters
    them. In this case we have one previously seen file and one never seen file.
    We expect a single call of the trigger_dag_mock for the unseen file.
    """
    files = tmpdir.mkdir("files")

    file1 = files.join("SMS_20160101.csv.gz")
    file1.write("blah")
    file2 = files.join("SMS_20160102.csv.gz")
    file2.write("blah")

    # add a some etl records
    file1_data = {
        "cdr_type": "sms",
        "cdr_date": parse("2016-01-01").date(),
        "state": "archive",
    }

    ETLRecord.set_state(
        cdr_type=file1_data["cdr_type"],
        cdr_date=file1_data["cdr_date"],
        state=file1_data["state"],
        session=session,
    )

    cdr_type_config = config["etl"]
    fake_dag_run = {}
    trigger_dag_mock = Mock()
    now = utcnow()
    uuid = uuid1()
    uuid_sans_underscore = str(uuid).replace("-", "")

    monkeypatch.setattr("etl.production_task_callables.uuid1", lambda: uuid)
    monkeypatch.setattr("etl.production_task_callables.utcnow", lambda: now)
    monkeypatch.setattr("etl.etl_utils.get_session", lambda: session)
    monkeypatch.setattr("etl.production_task_callables.trigger_dag", trigger_dag_mock)

    trigger__callable(
        dag_run=fake_dag_run, cdr_type_config=cdr_type_config, files_path=Path(files)
    )

    assert trigger_dag_mock.call_count == 1

    cdr_type = CDRType("sms")
    cdr_date = parse("2016-01-02")
    expected_conf = {
        "cdr_type": cdr_type,
        "cdr_date": cdr_date,
        "file_name": "SMS_20160102.csv.gz",
        "template_path": "etl/sms",
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type}_{str(cdr_date.date()).replace('-','')}",
    }

    trigger_dag_mock.assert_called_with(
        "etl_sms",
        conf=expected_conf,
        execution_date=now,
        run_id=f"SMS_20160102.csv.gz-{uuid}",
        replace_microseconds=False,
    )


def test_trigger__callable_multiple_triggers(tmpdir, session, monkeypatch):
    """
    Test that the trigger callable picks up files in files and is able to trigger
    multiple etl dag runs.
    """
    files = tmpdir.mkdir("files")

    file1 = files.join("SMS_20160101.csv.gz")
    file1.write("blah")
    file2 = files.join("CALLS_20160102.csv.gz")
    file2.write("blah")

    cdr_type_config = config["etl"]
    fake_dag_run = {}
    trigger_dag_mock = Mock()
    now = utcnow()
    uuid = uuid1()
    uuid_sans_underscore = str(uuid).replace("-", "")

    monkeypatch.setattr("etl.production_task_callables.uuid1", lambda: uuid)
    monkeypatch.setattr("etl.production_task_callables.utcnow", lambda: now)
    monkeypatch.setattr("etl.etl_utils.get_session", lambda: session)
    monkeypatch.setattr("etl.production_task_callables.trigger_dag", trigger_dag_mock)

    trigger__callable(
        dag_run=fake_dag_run, cdr_type_config=cdr_type_config, files_path=Path(files)
    )

    assert trigger_dag_mock.call_count == 2

    cdr_type_file1 = CDRType("sms")
    cdr_date_file1 = parse("2016-01-01")
    expected_conf_file1 = {
        "cdr_type": cdr_type_file1,
        "cdr_date": cdr_date_file1,
        "file_name": "SMS_20160101.csv.gz",
        "template_path": "etl/sms",
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type_file1}_{str(cdr_date_file1.date()).replace('-','')}",
    }

    cdr_type_file2 = CDRType("calls")
    cdr_date_file2 = parse("2016-01-02")
    expected_conf_file2 = {
        "cdr_type": cdr_type_file2,
        "cdr_date": cdr_date_file2,
        "file_name": "CALLS_20160102.csv.gz",
        "template_path": "etl/calls",
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type_file2}_{str(cdr_date_file2.date()).replace('-','')}",
    }

    trigger_dag_mock.assert_any_call(
        "etl_sms",
        conf=expected_conf_file1,
        execution_date=now,
        run_id=f"SMS_20160101.csv.gz-{uuid}",
        replace_microseconds=False,
    )

    trigger_dag_mock.assert_any_call(
        "etl_calls",
        conf=expected_conf_file2,
        execution_date=now,
        run_id=f"CALLS_20160102.csv.gz-{uuid}",
        replace_microseconds=False,
    )
