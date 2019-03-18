# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Grid


def test_grid_column_names():
    """Test that Grid's column_names property is correct."""
    g = Grid(100)
    assert g.head(0).columns.tolist() == g.column_names


def test_has_correct_size(get_length):
    """
    flowmachine.Grid returns the correct number of grid chunks.
    """
    grid = Grid(200)
    assert get_length(grid) == 15


def test_grid_geojson():
    """Test that a Grid can be exported to geojson."""
    grid = Grid(200)
    js = grid.to_geojson()
    assert len(js["features"]) == 15
