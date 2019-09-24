# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the PopulationWeightedOpportunities() class.
"""
import pandas as pd
import pytest

from flowmachine.core.errors import UnstorableQueryError
from flowmachine.models import PopulationWeightedOpportunities
from flowmachine.models.pwo import PWO


@pytest.mark.usefixtures("skip_datecheck")
def test_returns_correct_results(get_dataframe):
    """
    PopulationWeightedOpportunities().run() returns correct result set.
    """
    results = get_dataframe(PWO("2016-01-01", "2016-01-07"))
    set_df = results.set_index("site_id_from")
    assert set_df.loc["0xqNDj"]["site_id_to"].values[1] == "8wPojr"
    assert set_df.loc["0xqNDj"]["site_id_to"].values[3] == "B8OaG5"
    assert set_df.loc["0xqNDj"]["site_id_to"].values[7] == "DonxkP"

    assert set_df.loc["0xqNDj"]["prediction"].values[1] == pytest.approx(
        0.00428076166366159
    )
    assert set_df.loc["0xqNDj"]["prediction"].values[3] == pytest.approx(
        0.0160635939352117
    )
    assert set_df.loc["0xqNDj"]["prediction"].values[7] == pytest.approx(
        0.0069088242040108
    )

    assert set_df.loc["0xqNDj"]["probability"].values[1] == pytest.approx(
        0.00194580075620981
    )
    assert set_df.loc["0xqNDj"]["probability"].values[3] == pytest.approx(
        0.00730163360691442
    )
    assert set_df.loc["0xqNDj"]["probability"].values[7] == pytest.approx(
        0.00314037463818673
    )


@pytest.mark.usefixtures("skip_datecheck")
def test_run_with_location_vector(get_dataframe):
    """
    PopulationWeightedOpportunities().run() takes a location probability vector.
    """
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-07")
    set_df = get_dataframe(
        p.run(
            departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
            ignore_missing=True,
        )
    )
    set_df = set_df.set_index("site_id_from")
    assert set_df.loc["0xqNDj"]["site_id_to"].values[1] == "8wPojr"
    assert set_df.loc["0xqNDj"]["site_id_to"].values[3] == "B8OaG5"
    assert set_df.loc["0xqNDj"]["site_id_to"].values[7] == "DonxkP"

    assert set_df.loc["0xqNDj"]["prediction"].values[1] == pytest.approx(
        0.0385268549729543
    )
    assert set_df.loc["0xqNDj"]["prediction"].values[3] == pytest.approx(
        0.144572345416906
    )
    assert set_df.loc["0xqNDj"]["prediction"].values[7] == pytest.approx(
        0.0621794178360972
    )

    assert set_df.loc["0xqNDj"]["probability"].values[1] == pytest.approx(
        0.00194580075620981
    )
    assert set_df.loc["0xqNDj"]["probability"].values[3] == pytest.approx(
        0.00730163360691442
    )
    assert set_df.loc["0xqNDj"]["probability"].values[7] == pytest.approx(
        0.00314037463818673
    )


def test_error_raised_if_location_vector_incomplete():
    """
    PopulationWeightedOpportunities().run() raises error if location vector incomplete.
    """
    with pytest.raises(ValueError):
        p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02").run(
            departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
            ignore_missing=False,
        )


def test_pwo_result_storing():
    """Test that we can store a pwo model result and it will be retrieved for the next run"""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    r = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    ).store()
    r.result()
    r2 = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    assert r2.is_stored


def test_model_result_funcs():
    """Test overridden method for ModelResult"""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    for ix, r in enumerate(mr):
        pass
    assert ix + 1 == len(mr)
    mr2 = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    mr2.store().result()
    for ix, r in enumerate(mr2):
        pass
    assert ix + 1 == len(mr)


def test_model_result_make_query():
    """Test that make query returns tablename and stores."""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    qur = mr._make_query()
    assert mr.is_stored
    assert mr.fully_qualified_table_name in qur


def test_model_result_cols():
    """Test model result object has right columns."""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    cols = [
        "site_id_from",
        "version_from",
        "lon_from",
        "lat_from",
        "site_id_to",
        "version_to",
        "lon_to",
        "lat_to",
        "prediction",
        "probability",
    ]
    assert mr.column_names == cols
    mr.store().result()
    assert mr.column_names == cols


def test_model_result_stored_len():
    """Test len of the stored model result is correct"""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    l = len(mr)
    mr.store().result()
    assert l == len(mr)


def test_get_stored():
    """Test that get_stored works for ModelResults."""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    mr.store().result()
    assert sum(1 for x in PopulationWeightedOpportunities.get_stored()) == 1


def test_model_result_store_dependencies():
    """
    Test that storing a ModelResult with store_dependencies=True stores the model's (storable) dependencies.
    """
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    deps = mr.dependencies
    # Check that the dependencies aren't all already stored
    assert not all([dep.is_stored for dep in deps])
    mr.store(store_dependencies=True).result()
    for dep in deps:
        try:
            dep.table_name
            assert dep.is_stored
        except NotImplementedError:
            assert not dep.is_stored


def test_model_result_string_rep():
    """Test that ModelResult string rep has name of the model class"""
    p = PopulationWeightedOpportunities("2016-01-01", "2016-01-02")
    mr = p.run(
        departure_rate_vector=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        ignore_missing=True,
    )
    assert "PopulationWeightedOpportunities" in str(mr)
