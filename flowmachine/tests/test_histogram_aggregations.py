# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np

from flowmachine.features import Displacement, RadiusOfGyration, daily_location
from flowmachine.features.utilities.histogram_aggregations import HistogramAggregation
from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.utils import list_of_dates


def test_create_histogram_using_int_bins_value(get_dataframe):
    """
    Create histogram using one bins value.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(locations=RoG, bins=5)
    df = get_dataframe(agg)
    numpy_histogram = np.histogram(get_dataframe(RoG).value, bins=5)
    # assert set(numpy_histogram[0].tolist()) == set(df.value.tolist())


def test_create_histogram_using_list_of_bins_values(get_dataframe):
    """
    Create histogram using list of bins values.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(locations=RoG, bins=[10, 20, 30, 40, 50, 60])
    df = get_dataframe(agg)
    numpy_histogram = np.histogram(
        get_dataframe(RoG).value, bins=[10, 20, 30, 40, 50, 60]
    )
    # assert set(numpy_histogram[0].tolist()) == set(df.value.tolist())


def test_create_histogram_using_bins_and_range_values(get_dataframe):
    """
    Create histogram using one bins and range values.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")

    agg = HistogramAggregation(locations=RoG, bins=5, ranges=(130.00, 230.00))
    df = get_dataframe(agg)
    numpy_histogram = np.histogram(
        get_dataframe(RoG).value, bins=5, range=(130.00, 230.00)
    )

    assert set(numpy_histogram[0].tolist()) == set(df.value.tolist())
