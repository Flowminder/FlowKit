# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
conftest for unit tests
"""
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl.model import Base, ETLRecord  # pylint: disable=unused-import

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

    def __init__(self, task_id):
        self.task_id = task_id


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

    def fake_task_instance(*, task_id):
        return FakeTaskInstance(task_id=task_id)

    return fake_task_instance


@pytest.fixture(scope="function")
def session():
    """
    Fixture to yield a session to an in memory DB with
    correct model in place.
    """
    engine = create_engine("sqlite:///:memory:")
    engine.execute(f"ATTACH DATABASE ':memory:' AS etl;")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    returned_session = Session()
    yield returned_session
    returned_session.close()
