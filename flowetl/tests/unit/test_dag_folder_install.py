from click.testing import CliRunner


def test_copies_to_dag_folder(tmpdir, airflow_home):
    runner = CliRunner()
    from flowetl.cli import main

    dags_folder = tmpdir / "dags"
    dags_folder.mkdir()
    result = runner.invoke(main, [str(dags_folder)],)
    assert (dags_folder / "flowetl").exists()
