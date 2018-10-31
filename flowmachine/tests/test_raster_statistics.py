# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the RasterStatistics utilities.
"""
import pandas as pd

from unittest import TestCase
import pytest

from flowmachine.core import Table
from flowmachine.features.raster import RasterStatistics


class RasterStatisticsTestCase(TestCase):
    """
    Series of tests for the RasterStatistics() utility
    methods.
    """

    def setUp(self):
        """
        Sets up test environment.
        """
        #
        #  Here we use the results calculated manually using
        #  the method `zonal_stats` which come from the
        #  Python package `rasterio`. Those results
        #  are generally considered correct by the team
        #  so we will use them to compare our PostGIS
        #  results against.
        #
        self.rasterio_results = pd.read_csv("tests/data/rasterio_worldpop_results.csv")

    def test_computes_expected_total_value(self):
        """
        RasterStatistics() computes correct total value of raster.
        """
        r = RasterStatistics(raster="worldpop_gambia")
        result = r.get_dataframe()

        self.assertAlmostEqual(result["statistic"].iloc[0], 2083543.79777587)

    def test_computes_expected_clipping_values(self):
        """
        RasterStatistics() returns correct values when clipping vector and raster layers.
        """
        G = "district_c"
        vector = Table(schema="public", name="gambia_admin2")
        r = RasterStatistics(
            raster="worldpop_gambia", vector=vector, grouping_element=G
        )

        result = r.get_dataframe()
        for expected_result in self.rasterio_results.to_dict("records"):
            self.assertAlmostEqual(
                int(result[result[G] == expected_result["district"]].statistic.iloc[0]),
                expected_result["value"],
            )

    def test_raises_notimplemented_when_wrong_statistic_requested(self):
        """
        RasterStatistics() raises NotImplementedError if wrong statistic requested.
        """
        G = "district_c"
        with self.assertRaises(NotImplementedError):
            r = RasterStatistics(
                raster="worldpop_gambia",
                vector="gambia_admin2",
                grouping_element=G,
                statistic="mean",
            )

    def test_raises_valueerror_when_grouping_element_not_provided(self):
        """
        RasterStatistics() raises ValueError when `grouping_element` not provided.
        """
        G = None
        with self.assertRaises(ValueError):
            r = RasterStatistics(
                raster="worldpop_gambia", vector="gambia_admin2", grouping_element=None
            )

    def test_failure_with_no_grouping_layer(self):
        """
        RasterStatistics() checks that we can get column names when grouping layer omitted.
        """
        r = RasterStatistics(raster="worldpop_gambia")
        r.column_names

    def test_raster_statistics_column_names_novector(self):
        """
        Test that column_names property matches head(0) for RasterStatistics
        when vector is None
        """
        r = RasterStatistics(raster="worldpop_gambia")
        assert r.head(0).columns.tolist() == r.column_names

    def test_raster_statistics_column_names_vector(self):
        """
        Test that column_names property matches head(0) for RasterStatistics
        when vector is not None
        """
        vector = Table(schema="public", name="gambia_admin2")
        r = RasterStatistics(
            raster="worldpop_gambia", vector=vector, grouping_element="district_c"
        )
        assert r.head(0).columns.tolist() == r.column_names
