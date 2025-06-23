# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
from pathlib import Path, PosixPath

import pytest

staging_qa_checks = {
    cdr_type: {
        f"{f.stem}.staging": str(f.name)
        for f in sorted(
            (
                Path(__file__).parent.parent.parent
                / "flowetl"
                / "flowetl"
                / "qa_checks"
                / "staging"
            ).glob(f"{cdr_type}/*.sql")
        )
    }
    for cdr_type in {"calls", "sms", "mds", "topups", "cell_info", "."}
}

extract_qa_checks = {
    f.stem: str(f.name)
    for f in sorted(
        (
            Path(__file__).parent.parent.parent
            / "flowetl"
            / "flowetl"
            / "qa_checks"
            / "extract"
        ).glob("**/*.sql")
    )
}

final_qa_checks = {
    cdr_type: {
        f"{f.stem}.final": str(f.name)
        for f in sorted(
            (
                Path(__file__).parent.parent.parent
                / "flowetl"
                / "flowetl"
                / "qa_checks"
                / "final"
            ).glob(f"{cdr_type}/*.sql")
        )
    }
    for cdr_type in {"calls", "sms", "mds", "topups", "."}
}


@pytest.mark.parametrize(
    "qa_stage,expected",
    [
        ("final", final_qa_checks["."]),
        ("extract", {}),
        ("staging", staging_qa_checks["."]),
    ],
)
def test_default_qa_checks_found(qa_stage, expected):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    dag = DAG("DUMMY_DAG", start_date=datetime.now())
    check_operators = get_qa_checks(dag=dag, stage=qa_stage)
    assert {op.task_id: op.sql for op in check_operators} == expected


def test_name_suffix_added(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks" / "final" / "calls").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "final" / "calls" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
            template_searchpath=str(tmpdir),
            params=dict(cdr_type="calls"),
        )
    )
    assert any(op for op in check_operators if op.task_id == "DUMMY_CHECK.calls.final")


def test_additional_checks_collected(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks" / "final").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "final" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG("DUMMY_DAG", start_date=datetime.now(), template_searchpath=str(tmpdir))
    )

    assert len(check_operators) > len(final_qa_checks["."])


def test_additional_checks_collected_from_dag_folder():
    from airflow import DAG, settings
    from flowetl.util import get_qa_checks

    dag_folder = Path(settings.DAGS_FOLDER) / "ETL_SUBDIR_OF_DAGS_FOLDER"
    checks_folder = dag_folder / "qa_checks" / "final"
    checks_folder.mkdir(parents=True)
    (checks_folder / "DUMMY_CHECK.sql").touch()
    dag = DAG("DUMMY_DAG", start_date=datetime.now())
    dag.fileloc = dag_folder / "DUMMY_DAG.py"
    check_operators = get_qa_checks(dag=dag)

    assert len(check_operators) > len(final_qa_checks["."])


def test_additional_checks_collected_in_subdirs(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "qa_checks" / "final" / "calls").mkdir(parents=True)
    Path(tmpdir / "qa_checks" / "final" / "calls" / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG("DUMMY_DAG", start_date=datetime.now(), template_searchpath=str(tmpdir))
    )

    assert len(check_operators) == len(final_qa_checks["."])

    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
            template_searchpath=str(tmpdir),
            params=dict(cdr_type="calls"),
        ),
    )

    assert len(check_operators) > len(final_qa_checks["calls"])


def test_additional_checks_collected_if_specified(tmpdir):
    from airflow import DAG
    from flowetl.util import get_qa_checks

    Path(tmpdir / "DUMMY_CHECK.sql").touch()
    check_operators = get_qa_checks(
        dag=DAG(
            "DUMMY_DAG",
            start_date=datetime.now(),
        ),
        additional_qa_check_paths=[str(tmpdir)],
    )

    assert len(check_operators) > len(final_qa_checks["."])


def test_module_path_added_to_dag_template_locations():
    from airflow import DAG
    from flowetl.util import get_qa_checks

    dag = DAG("DUMMY_DAG", start_date=datetime.now())
    get_qa_checks(dag=dag)
    assert (
        Path(__file__).parent.parent.parent
        / "flowetl"
        / "flowetl"
        / "qa_checks"
        / "final"
        in dag.template_searchpath
    )


def test_uses_context_dag():
    from airflow import DAG
    from flowetl.util import get_qa_checks

    with DAG("DUMMY_DAG", start_date=datetime.now()) as dag:
        get_qa_checks()
    assert (
        Path(__file__).parent.parent.parent
        / "flowetl"
        / "flowetl"
        / "qa_checks"
        / "final"
        in dag.template_searchpath
    )


def test_error_on_no_dag():
    from flowetl.util import get_qa_checks

    with pytest.raises(
        TypeError, match="Must set dag argument or be in a dag context manager."
    ):
        get_qa_checks()


@pytest.mark.parametrize(
    "paths, expected",
    [
        (
            [
                Path("/A/B/C/1.txt"),
                Path("/A/B/C/2.txt"),
                Path("/A/B/D/1.txt"),
                Path("/D/B/C/1.txt"),
                Path("/D/1.txt"),
                Path("/A/B/C/3.txt"),
            ],
            {
                PosixPath("/"): [
                    "A/B/C/1.txt",
                    "A/B/D/1.txt",
                    "D/B/C/1.txt",
                    "D/1.txt",
                ],
                PosixPath("/A/B/C"): ["2.txt", "3.txt"],
            },
        ),
        (
            [Path("/A/B/C/1.txt"), Path("/A/B/C/2.txt")],
            {PosixPath("/A/B/C"): ["1.txt", "2.txt"]},
        ),
    ],
)
def test_path_disambiguation(paths, expected):
    from flowetl.util import disambiguate_paths

    assert disambiguate_paths(paths) == expected
