import pandas as pd
import pytest

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)


@pytest.fixture
def get_dataframe(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        query.get_query(), con=flowmachine_connect.engine
    )


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    return diff_reporter_factory.get("opendiff")
    # return diff_reporter_factory.get_first_working()
