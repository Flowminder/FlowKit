# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import logging
import time
import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from autoflow.model import Base, WorkflowRuns


@pytest.fixture(scope="session")
def postgres_test_db():
    """
    Fixture to yield a PostgreSQL database to be used in the unit tests.
    The database will only be created once per test session, but the
    workflow_runs table will be cleared for each test.
    """
    postgres_test_db = testing.postgresql.Postgresql()
    engine = create_engine(postgres_test_db.url())

    from sqlalchemy_utils import database_exists, create_database

    if not database_exists(engine.url):
        create_database(engine.url)
    Base.metadata.create_all(bind=engine, tables=[WorkflowRuns.__table__])

    yield postgres_test_db
    postgres_test_db.stop()


@pytest.fixture(scope="function")
def session(postgres_test_db):
    """
    Fixture to yield a session to the test PostgreSQL database. This clears
    the workflow_runs table for each test.
    """
    engine = create_engine(postgres_test_db.url())
    engine.execute(f"TRUNCATE TABLE {WorkflowRuns.__table__};")
    Session = sessionmaker(bind=engine)
    returned_session = Session()
    yield returned_session
    returned_session.close()


@pytest.fixture(scope="function")
def sqlite_session():
    """
    Fixture to yield a session to a temporary sqlite database.
    This creates the workflow_runs table before yielding a session.
    """
    engine = create_engine("sqlite:///")
    Base.metadata.drop_all(bind=engine, tables=[WorkflowRuns.__table__])
    Base.metadata.create_all(bind=engine, tables=[WorkflowRuns.__table__])
    Session = sessionmaker(bind=engine)
    returned_session = Session()
    yield returned_session
    returned_session.close()
    Base.metadata.drop_all(bind=engine, tables=[WorkflowRuns.__table__])


@pytest.fixture(scope="session")
def test_logger():
    """
    Logger to use when testing prefect tasks.
    """
    logger = logging.getLogger("autoflow_tests")
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s | %(message)s"
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
