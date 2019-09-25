# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for models.
"""
from typing import List

import pandas as pd
import pytest

from flowmachine.core import Query
from flowmachine.core.model import Model, model_result
from flowmachine.core.model_result import ModelResult


class DummyQuery(Query):
    """
    Dummy query which can be used for testing and written to the db.,
    """

    def __init__(self, dummy_param):
        self.dummy_param = dummy_param

    @property
    def column_names(self) -> List[str]:
        return ["foo"]

    def _make_query(self):
        return "SELECT 1 as foo"


class DummyModel(Model):
    def __init__(self, dummy_dependency, dummy_list, dummy_dict):
        self.dummy_dependency = dummy_dependency
        self.dummy_list = dummy_list
        self.dummy_dict = dummy_dict

    @model_result
    def run(self, dummy_arg):
        return pd.DataFrame({"DUMMY_COLUMN": [0, 1, 2, 3]})


def test_splitting():
    """
    Test that queries and non-query args are split.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {"dummy_key": 1})
    qs, args = ModelResult._split_query_objects(dummy)
    assert [dq] == qs
    assert [("dummy_key", 1), ("dummy_list", 1)] == args


def test_model_result_can_be_stored():
    """
    Test that the result of a model run can be stored.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(1)
    dummy_result.store().result()
    assert dummy_result.is_stored


def test_model_result_can_be_stored_with_deps():
    """
    Test that result of a model run and any query dependencies can be stored.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(1)
    dummy_result.store(store_dependencies=True).result()
    assert dummy_result.is_stored
    assert dq.is_stored


def test_model_result_is_retrieved():
    """
    Test that model results are got from db if already stored.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    del dummy_result
    assert dummy.run(2).is_stored


def test_error_with_no_df():
    """
    Test that an error is raised if we try to store a model result that
    has not been calculated.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    with pytest.raises(ValueError):
        ModelResult(dummy, run_args=[2]).store().result()


def test_make_query_stores():
    """
    Test that calling _make_query on a model result causes it to be stored.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    assert dummy_result.is_stored
    dummy_result.invalidate_db_cache()
    assert not dummy_result.is_stored
    dummy_result._make_query()
    assert dummy_result.is_stored


def test_repr():
    """
    Test that the string representation of a model is correct.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    assert (
        "Model result of type DummyModel: run(2)"
        == ModelResult(dummy, run_args=[2]).__repr__()
    )


def test_class_get_stored():
    """
    Test that stored model results can be retrieved.
    """

    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    assert [dummy_result.query_id] == [q.query_id for q in ModelResult.get_stored()]


def test_model_result_column_names():
    """
    Test that model result columns are correct
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    assert ["DUMMY_COLUMN"] == dummy_result.column_names


def test_model_result_column_names_without_df():
    """
    Test that model result columns are correct when coming from db.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    del dummy_result._df
    assert ["DUMMY_COLUMN"] == dummy_result.column_names


def test_model_result_len():
    """
    Test that model result length is correct.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    assert 4 == len(dummy_result)


def test_model_result_len_without_df():
    """
    Test that model result length is correct when coming from db.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    del dummy_result._df
    assert 4 == len(dummy_result)


def test_model_result_iteration():
    """
    Test that we can iterate over model results.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    df = dummy_result._df
    for row_a, row_b in zip(df.itertuples(index=False), dummy_result):
        assert row_a == row_b


def test_model_result_iteration_with_no_df():
    """
    Test that we can iterate over model results from the db.
    """
    dq = DummyQuery("DUMMY")
    dummy = DummyModel(dq, [1], {1: 1})
    dummy_result = dummy.run(2)
    dummy_result.store().result()
    df = dummy_result._df
    del dummy_result._df
    for row_a, row_b in zip(df.itertuples(index=False), dummy_result):
        assert tuple(row_a) == row_b
