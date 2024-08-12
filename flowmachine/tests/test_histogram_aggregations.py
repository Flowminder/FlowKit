# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import pytest

from flowmachine.core import CustomQuery
from flowmachine.features import RadiusOfGyration
from flowmachine.features.nonspatial_aggregates.histogram_aggregation import (
    HistogramAggregation,
)


def test_non_default_value_column(get_dataframe):
    """
    Test a non-default value column.
    """
    query = CustomQuery(
        "SELECT * FROM generate_series(0, 100) AS t(non_default_value_column)",
        column_names=["non_default_value_column"],
    )

    agg = HistogramAggregation(
        metric=query, bins=5, value_column="non_default_value_column"
    )
    df = get_dataframe(agg)
    numpy_histogram, numpy_bins = np.histogram(
        get_dataframe(query).non_default_value_column, bins=5
    )
    assert df.value.sum() == len(get_dataframe(query))
    assert numpy_histogram.tolist() == df.value.tolist()
    assert numpy_bins.tolist()[:-1] == pytest.approx(df.lower_edge.tolist())
    assert numpy_bins.tolist()[1:] == pytest.approx(df.upper_edge.tolist())


def test_create_histogram_censors(get_dataframe):
    """
    Histogram should be censored if any bin has a count below 15.
    """
    query = CustomQuery(
        "SELECT * FROM generate_series(0, 10) AS t(value)", column_names=["value"]
    )

    agg = HistogramAggregation(metric=query, bins=5)
    df = get_dataframe(agg)

    assert len(df) == 1
    assert df.value[0] is None
    assert df.lower_edge[0] is None
    assert df.upper_edge[0] is None


def test_create_histogram_using_int_bins_value(get_dataframe):
    """
    Create histogram using one bins value.
    """
    radius_of_gyration = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(metric=radius_of_gyration, bins=5, censor=False)
    df = get_dataframe(agg)
    numpy_histogram, numpy_bins = np.histogram(
        get_dataframe(radius_of_gyration).value, bins=5
    )
    assert df.value.sum() == len(get_dataframe(radius_of_gyration))
    assert numpy_histogram.tolist() == df.value.tolist()
    assert numpy_bins.tolist()[:-1] == pytest.approx(df.lower_edge.tolist())
    assert numpy_bins.tolist()[1:] == pytest.approx(df.upper_edge.tolist())


def test_create_histogram_using_list_of_bins_values(get_dataframe):
    """
    Create histogram using list of bins values.
    """
    radius_of_gyration = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(
        metric=radius_of_gyration, bins=[10, 20, 30, 40, 50, 60], censor=False
    )
    df = get_dataframe(agg)
    numpy_histogram, numpy_bins = np.histogram(
        get_dataframe(radius_of_gyration).value, bins=[10, 20, 30, 40, 50, 60]
    )
    assert numpy_histogram.tolist() == df.value.tolist()
    assert numpy_bins.tolist()[:-1] == pytest.approx(df.lower_edge.tolist())
    assert numpy_bins.tolist()[1:] == pytest.approx(df.upper_edge.tolist())


def test_create_histogram_using_bins_and_range_values(get_dataframe):
    """
    Create histogram using one bins and range values.
    """
    radius_of_gyration = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(
        metric=radius_of_gyration, bins=5, range=(130.00, 230.00), censor=False
    )
    df = get_dataframe(agg)
    numpy_histogram, numpy_bins = np.histogram(
        get_dataframe(radius_of_gyration).value, bins=5, range=(130.00, 230.00)
    )

    assert numpy_histogram.tolist() == df.value.tolist()
    assert numpy_bins.tolist()[:-1] == pytest.approx(df.lower_edge.tolist())
    assert numpy_bins.tolist()[1:] == pytest.approx(df.upper_edge.tolist())


def test_create_histogram_using_bins_list_and_range_values(get_dataframe):
    """
    Create histogram using list of bins and range values (checking for consistency with numpy).
    """
    radius_of_gyration = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(
        metric=radius_of_gyration,
        bins=[10, 20, 30, 40, 50, 60],
        range=(130.00, 230.00),
        censor=False,
    )
    df = get_dataframe(agg)
    numpy_histogram, numpy_bins = np.histogram(
        get_dataframe(radius_of_gyration).value,
        bins=[10, 20, 30, 40, 50, 60],
        range=(130.00, 230.00),
    )

    assert numpy_histogram.tolist() == df.value.tolist()
    assert numpy_bins.tolist()[:-1] == pytest.approx(df.lower_edge.tolist())
    assert numpy_bins.tolist()[1:] == pytest.approx(df.upper_edge.tolist())


@pytest.mark.parametrize(
    "param_name, param_value, expected_exception",
    [
        (
            "value_column",
            "NOT_A_COLUMN",
            "'NOT_A_COLUMN' is not a column in this query. Must be one of '\['subscriber', 'value'\]'",
        ),
        (
            "bins",
            "NOT_A_BIN_TYPE",
            "Bins should be an integer or list of numeric values.",
        ),
        ("range", "NOT_A_RANGE_TYPE", "Range should be tuple of two values or None."),
    ],
)
def test_histogram_param_value_errors(param_name, param_value, expected_exception):
    radius_of_gyration = RadiusOfGyration("2016-01-01", "2016-01-02")

    args = dict(bins=10, metric=radius_of_gyration)
    args[param_name] = param_value
    with pytest.raises(ValueError, match=expected_exception):
        HistogramAggregation(**args)
