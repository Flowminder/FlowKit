import pytest


class FakeDagRun:
    def __init__(self, conf):
        self.conf = conf


@pytest.fixture(scope="function")
def create_fake_dag_run():
    def fake_dag_run(*, conf):
        return FakeDagRun(conf=conf)

    return fake_dag_run
