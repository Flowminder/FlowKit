from flowetl.staging.staging import StagingManager, ExampleMappingStep, get_cursor
import docker
import tarfile
from tempfile import NamedTemporaryFile
from pathlib import Path


import pytest


@pytest.fixture
def flowmachine_env(monkeypatch):
    with open("../../../development_environment", "r") as env_file:
        for line in env_file.readlines():
            if "=" in line:
                key, sep, value = line.partition("=")
                if sep == "=":
                    monkeypatch.setenv(key, value.strip())


# I doubt these fixtures will run happily on the CI, but it's good enough for now
@pytest.fixture
def flowdb_container_connection():
    client = docker.from_env()
    flowdb_container = client.containers.list(filters={"name": "flowdb"})[0]
    yield flowdb_container


@pytest.fixture
def flowdb_with_test_csvs(flowdb_container_connection):
    with NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w") as tfile:
            for csv_file in Path("../static_csvs").iterdir():
                tfile.add(csv_file)
        flowdb_container_connection.exec_run("mkdir -p /test_data/static_csvs")
        flowdb_container_connection.put_archive("/test_data/static_csvs", tmp)
    yield flowdb_container_connection
    flowdb_container_connection.exec_run("rm -r /test_data")


def test_csv_ingestion(flowmachine_env, flowdb_with_test_csvs):
    csv_dir = "/test_data/static_csvs"
    opt_out_path = "/test_data/static_csvs/opt_out_list.csv"

    archive = StagingManager(csv_dir, opt_out_list_path=opt_out_path)
    archive.load_csv_on_date("2021_09_29")
    with get_cursor(archive.db_con) as cur:
        cur.execute(
            """
            SELECT * FROM reduced.sightings_2021_09_29
        """
        )
        reduced_records = cur.fetchall()
        assert len(reduced_records) == 36  # From test csvs
        cur.execute(
            """
            SELECT * FROM reduced.cell_location_mapping
        """
        )
        mapping = cur.fetchall()
        cell_ids, location_ids = zip(*mapping)
        assert cell_ids == location_ids


def test_csv_with_clustering(flowmachine_env, flowdb_with_test_csvs):
    csv_dir = "/test_data/static_csvs"
    opt_out_path = "/test_data/static_csvs/opt_out_list.csv"

    sm = StagingManager(
        csv_dir,
        opt_out_list_path=opt_out_path,
        tower_clustering_class=ExampleMappingStep,
    )
    sm.load_csv_on_date("2021_09_29")
    with get_cursor(sm.db_con) as cur:
        cur.execute(
            """
            SELECT * FROM reduced.cell_location_mapping
        """
        )
        mapping = cur.fetchall()
        cell_ids, location_ids = zip(*mapping)
        assert all([l_id in location_ids for l_id in ("A", "B", "C")])
