# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os

from datetime import datetime
from pathlib import Path

import pytest

qa_checks = {
    f.stem: str(Path("qa_checks") / f.name)
    for f in sorted(
        (
            Path(__file__).parent.parent.parent
            / "flowetl"
            / "flowetl"
            / "qa_checks"
            / "qa_checks"
        ).glob("*.sql")
    )
}


def test_default_qa_checks_found():
    from airflow import DAG
    from flowetl.util import get_qa_checks

    dag = DAG("DUMMY_DAG", start_date=datetime.now())
    check_operators = get_qa_checks(dag=dag)
    assert {op.task_id: op.sql for op in check_operators} == qa_checks


def test_name_suffix_added(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks" / "calls").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "calls" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
            template_searchpath=str(tmpdir),
            params=dict(cdr_type="calls"),
        )
    )
    assert any(op for op in check_operators if op.task_id == "DUMMY_CHECK.calls")


def test_additional_checks_collected(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks").mkdir()
    Path(tmpdir / "qa_checks" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG("DUMMY_DAG", start_date=datetime.now(), template_searchpath=str(tmpdir))
    )

    assert len(check_operators) > len(qa_checks)


def test_additional_checks_collected_from_home():
    from airflow import DAG, settings
    from flowetl.util import get_qa_checks

    checks_folder = Path(settings.DAGS_FOLDER) / "qa_checks"
    checks_folder.mkdir(parents=True)
    (checks_folder / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(dag=DAG("DUMMY_DAG", start_date=datetime.now()))

    assert len(check_operators) > len(qa_checks)


def test_additional_checks_collected_in_subdirs(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks" / "calls").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "calls" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG("DUMMY_DAG", start_date=datetime.now(), template_searchpath=str(tmpdir))
    )

    assert len(check_operators) == len(qa_checks)

    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
            template_searchpath=str(tmpdir),
            params=dict(cdr_type="calls"),
        ),
    )

    assert len(check_operators) > len(qa_checks)


def test_additional_checks_collected_if_specified(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
        ),
        additional_qa_check_paths=[str(tmpdir)],
    )

    assert len(check_operators) > len(qa_checks)


def test_module_path_added_to_dag_template_locations():
    from airflow import DAG
    from flowetl.util import get_qa_checks

    dag = DAG("DUMMY_DAG", start_date=datetime.now())
    get_qa_checks(dag=dag)
    assert (
        str(Path(__file__).parent.parent.parent / "flowetl" / "flowetl" / "qa_checks")
        in dag.template_searchpath
    )


def test_uses_context_dag():
    from airflow import DAG
    from flowetl.util import get_qa_checks

    with DAG("DUMMY_DAG", start_date=datetime.now()) as dag:
        get_qa_checks()
    assert (
        str(Path(__file__).parent.parent.parent / "flowetl" / "flowetl" / "qa_checks")
        in dag.template_searchpath
    )


def test_error_on_no_dag():
    from flowetl.util import get_qa_checks

    with pytest.raises(
        TypeError, match="Must set dag argument or be in a dag context manager."
    ):
        get_qa_checks()
