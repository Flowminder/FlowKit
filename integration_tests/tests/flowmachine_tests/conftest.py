import pandas as pd
import pytest

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)


@pytest.fixture
def get_dataframe(fm_conn):
    yield lambda query: pd.read_sql_query(
        query.get_query(), con=fm_conn.engine
    )


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    return diff_reporter_factory.get("opendiff")
    # return diff_reporter_factory.get_first_working()
