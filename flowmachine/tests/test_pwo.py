# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the PopulationWeightedOpportunities() class.
"""
import pandas as pd
import pytest

from flowmachine.features.location.pwo import PopulationWeightedOpportunities


@pytest.mark.usefixtures("skip_datecheck")
def test_returns_correct_results(get_dataframe):
    """
    PopulationWeightedOpportunities() returns correct result set.
    """
    results = get_dataframe(PopulationWeightedOpportunities("2016-01-01", "2016-01-07"))
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
    PopulationWeightedOpportunities takes a location probability vector.
    """
    set_df = get_dataframe(
        PopulationWeightedOpportunities(
            "2016-01-01",
            "2016-01-07",
            departure_rate=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
        )
    ).set_index("site_id_from")

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


def test_pwo_result_cols():
    """Test pwo result object has right columns."""
    mr = PopulationWeightedOpportunities(
        "2016-01-01",
        "2016-01-07",
        departure_rate=pd.DataFrame([{"site_id": "0xqNDj", "rate": 0.9}]),
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
