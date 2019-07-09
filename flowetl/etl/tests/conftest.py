# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
conftest for unit tests
"""
import os
import pytest
import testing.postgresql
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl.model import (
    Base,
    ETLRecord,
    ETLPostQueryOutcome,
)  # pylint: disable=unused-import

here = os.path.dirname(os.path.abspath(__file__))


# pylint: disable=too-few-public-methods
class FakeDagRun:
    """
    A fake DagRun object used for faking dag config
    """

    def __init__(self, conf=None):
        self.conf = conf


class FakeTaskInstance:
    """
    A fake TaskInstance object
    """

    def __init__(self, task_id=None, state=None):
        self.task_id = task_id
        self.state = state


@pytest.fixture(scope="function")
def create_fake_dag_run():
    """
    Returns a function that can be used to generate a fake DagRun with
    specific config
    """

    def fake_dag_run(*, conf=None):
        return FakeDagRun(conf=conf)

    return fake_dag_run


@pytest.fixture(scope="function")
def create_fake_task_instance():
    """
    Returns a function that can be used to generate a fake TaskInstance
    with specific task_id
    """

    def fake_task_instance(*, task_id=None, state=None):
        return FakeTaskInstance(task_id=task_id, state=state)

    return fake_task_instance


@pytest.fixture(scope="session")
def postgres_test_db():
    """
    Fixture to yield a PostgreSQL database to be used in the unit tests.
    The database will only be created once per test session, but the
    etl_records table will be cleared for each test.
    """
    postgres_test_db = testing.postgresql.Postgresql()
    engine = create_engine(postgres_test_db.url())

    from sqlalchemy.schema import CreateSchema
    from sqlalchemy_utils import database_exists, create_database

    if not database_exists(engine.url):
        create_database(engine.url)
    engine.execute(CreateSchema("etl"))
    Base.metadata.create_all(
        bind=engine, tables=[ETLRecord.__table__, ETLPostQueryOutcome.__table__]
    )
    engine.execute(
        """
        CREATE TABLE IF NOT EXISTS mds_raw_data_dump (
            imei TEXT,
            msisdn TEXT,
            event_time TIMESTAMPTZ,
            cell_id TEXT
        );
        """
    )

    yield postgres_test_db
    postgres_test_db.stop()


@pytest.fixture(scope="function")
def session(postgres_test_db):
    """
    Fixture to yield a session to the test PostgreSQL database. This clears
    the etl_records table for each test.
    """
    engine = create_engine(postgres_test_db.url())
    engine.execute(f"TRUNCATE TABLE {ETLRecord.__table__};")
    engine.execute(f"TRUNCATE TABLE {ETLPostQueryOutcome.__table__};")
    Session = sessionmaker(bind=engine)
    returned_session = Session()
    yield returned_session
    returned_session.close()


@pytest.fixture(scope="session")
def sample_config_dict():
    """
    Return sample config loaded from the file `flowetl/mounts/config/config.yml`.
    """
    sample_config_file = os.path.join(
        here, "..", "..", "mounts", "config", "config.yml"
    )
    with open(sample_config_file, "r") as f:
        cfg_dict = yaml.load(f.read(), Loader=yaml.SafeLoader)
    return cfg_dict
