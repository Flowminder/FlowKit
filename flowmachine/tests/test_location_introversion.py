# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the LocationIntroversion() class.
"""


import pytest

from flowmachine.features.location import LocationIntroversion


@pytest.mark.usefixtures("skip_datecheck")
def test_location_introversion_column_names(exemplar_level_param):
    """ Test that column_names property matches head(0)"""
    if exemplar_level_param["level"] == "versioned-site":
        pytest.skip(
            'The level "versioned-site" is currently not supported in the `LocationIntroversion()` class.'
        )
    li = LocationIntroversion("2016-01-01", "2016-01-07", **exemplar_level_param)
    assert li.head(0).columns.tolist() == li.column_names


def test_some_results(get_dataframe):
    """
    LocationIntroversion() returns a dataframe that contains hand-picked results.
    """
    df = get_dataframe(LocationIntroversion("2016-01-01", "2016-01-07", level="admin3"))
    set_df = df.set_index("name")
    assert round(set_df.loc["Dolpa"]["introversion"], 6) == pytest.approx(0.108517)
    assert round(set_df.loc["Myagdi"]["introversion"], 6) == pytest.approx(0.097884)
    assert round(set_df.loc["Ramechhap"]["extroversion"], 6) == pytest.approx(0.936842)


def test_lat_lng_introversion(get_dataframe):
    df = get_dataframe(
        LocationIntroversion("2016-01-01", "2016-01-07", level="lat-lon")
    )
    assert pytest.approx(0.0681818181818182) == df.introversion.max()
    assert 1.0 == df.extroversion.max()
    assert [28.2715052907426, 83.7762949093138] == df.sort_values("extroversion").iloc[
        -1
    ][["lat", "lon"]].tolist()


def test_no_result_is_greater_than_one(get_dataframe):
    """
    No results from LocationIntroversion()['introversion'] is greater than 1.
    """
    df = get_dataframe(LocationIntroversion("2016-01-01", "2016-01-07", level="admin3"))
    results = df[df["introversion"] > 1]
    assert len(results) == 0


def test_introversion_plus_extroversion_equals_one(get_dataframe):
    """
    LocationIntroversion()['introversion'] + ['extroversion'] equals 1.
    """
    df = get_dataframe(LocationIntroversion("2016-01-01", "2016-01-07", level="admin3"))
    df["addition"] = df["introversion"] + df["extroversion"]
    assert df["addition"].sum() == len(df)


def test_introversion_raises_notimplemented_error_with_versioned_site():
    """
    LocationIntroversion(level='versioned-site') raises a NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        LocationIntroversion("2016-01-01", "2016-01-07", level="versioned-site")
