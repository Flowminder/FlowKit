from flowetl.staging.staging import ArchiveManager
import pytest


@pytest.fixture
def flowmachine_env(monkeypatch):
    with open("../../../development_environment", "r") as env_file:
        for line in env_file.readlines():
            if "=" in line:
                key, sep, value = line.partition("=")
                if sep == "=":
                    monkeypatch.setenv(key, value.strip())


def test_csv_ingestion(flowmachine_env):
    # Need to mount this in the actual docker image somehow
    csv_dir = "/test_data/static_csvs"
    opt_out_path = "/test_data/static_csvs/opt_out_list.csv"
    archive = ArchiveManager(csv_dir, opt_out_list_path=opt_out_path)
    archive.retrieve_csv_on_date("2021_09_29")
