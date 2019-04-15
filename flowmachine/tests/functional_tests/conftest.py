import os
import pytest

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)

here = os.path.dirname(os.path.abspath(__file__))
flowkit_toplevel_dir = os.path.join(here, "..", "..", "..")


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    diff_reporter_factory.load(
        os.path.join(flowkit_toplevel_dir, "approvaltests_diff_reporters.json")
    )
    return diff_reporter_factory.get_first_working()
