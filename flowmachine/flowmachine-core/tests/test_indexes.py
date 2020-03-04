# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List
from unittest.mock import Mock

import pytest
from flowmachine_core.utility_queries.custom_query import CustomQuery
from flowmachine_core.core.context import get_db
from flowmachine_core.query_bases.query import Query


class IndexedQuery(Query):
    @property
    def column_names(self) -> List[str]:
        return self.id_cols + self.other_cols

    def __init__(self, id_cols, other_cols=[]):
        self.id_cols = id_cols
        self.other_cols = other_cols
        self.spatial_unit = Mock(location_id_columns=id_cols)
        super().__init__()

    def _make_query(self):
        cols = ", ".join(f"1 as {col}" for col in self.id_cols + self.other_cols)
        return f"SELECT {cols}"


def test_default_indexes():
    """
    Check that default indexing columns are correct
    """
    assert IndexedQuery(["pcod"], ["subscriber"]).index_cols == [
        ["pcod"],
        '"subscriber"',
    ]
    assert IndexedQuery(["lon", "lat"], ["subscriber"]).index_cols == [
        ["lon", "lat"],
        '"subscriber"',
    ]
    assert CustomQuery(
        "SELECT 1 as subscriber, 2 as another_column",
        column_names=["subscriber", "another_column"],
    ).index_cols == ['"subscriber"']


def test_index_created():
    """
    Check that an index actually gets created on storage.
    """
    indexed_test_query = IndexedQuery(["pcod"], ["subscriber"]).store().result()
    ix_qur = "SELECT * FROM pg_indexes WHERE tablename='{}'".format(
        indexed_test_query.fully_qualified_table_name.split(".")[1]
    )
    assert len(get_db().fetch(ix_qur)) == 2
