from flowmachine import connect
from flowmachine.core.context import get_db
from flowetl_generator.archive import ArchiveManager
import configparser
import pytest

@pytest.fixture
def flowmachine_env(monkeypatch):
    with open("../development_environment" ,'r') as env_file:
        for line in env_file.readlines():
            if "=" in line:
                key, sep, value = line.partition("=")
                if sep == "=":
                    monkeypatch.setenv(key, value.strip())

def test_csv_ingestion(flowmachine_env):
    connect()
    db = get_db().engine
    # Need to mount this in the actual docker image somehow
    csv_dir = "/test_data/static_csvs"
    archive = ArchiveManager(csv_dir)
    archive.csv_to_flowdb("2021_09_29")
