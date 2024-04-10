# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the Geography class.
"""

import geojson
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.features.spatial import Geography


def test_geography_column_names(exemplar_spatial_unit_param):
    """
    Test that column_names property matches head(0) for Geography.
    """
    if not exemplar_spatial_unit_param.has_geography:
        pytest.skip(f"{exemplar_spatial_unit_param} has no geography.")
    geo = Geography(exemplar_spatial_unit_param)
    assert geo.head(0).columns.tolist() == geo.column_names


def test_geography_raises_error():
    """
    Test that Geography raises an error for an invalid spatial unit.
    """
    with pytest.raises(InvalidSpatialUnitError):
        geo = Geography(make_spatial_unit("cell"))


@pytest.mark.parametrize(
    "make_spatial_unit_params",
    [
        {"spatial_unit_type": "versioned-cell"},
        {"spatial_unit_type": "versioned-site"},
        {"spatial_unit_type": "lon-lat"},
        {"spatial_unit_type": "admin", "level": 2},
        {"spatial_unit_type": "grid", "size": 5},
    ],
)
def test_valid_geojson(make_spatial_unit_params):
    """
    Check that valid geojson is returned.
    """
    spatial_unit = make_spatial_unit(**make_spatial_unit_params)
    geo = Geography(spatial_unit)
    assert geojson.loads(geo.to_geojson_string()).is_valid
