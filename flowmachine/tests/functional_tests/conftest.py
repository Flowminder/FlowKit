import pytest

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    return diff_reporter_factory.get("opendiff")
    # return diff_reporter_factory.get_first_working()
