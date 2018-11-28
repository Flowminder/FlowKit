# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features import CellToPolygon, CellToAdmin, CellToGrid
import pytest


@pytest.mark.parametrize(
    "mapper, args",
    [
        (CellToAdmin, {"level": "admin3"}),
        (CellToAdmin, {"level": "admin3", "column_name": "admin3pcod"}),
        (
            CellToPolygon,
            {"column_name": "admin3name", "polygon_table": "geography.admin3"},
        ),
        (
            CellToPolygon,
            {
                "column_name": "id",
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
        (
            CellToPolygon,
            {
                "column_name": "id",
                "polygon_table": "SELECT * FROM infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
        (CellToGrid, {"size": 5}),
    ],
)
def test_cell_to_x_mapping_column_names(mapper, args):
    """Test that the CellToX mappers have accurate column_names properties."""
    instance = mapper(**args)
    assert instance.head(0).columns.tolist() == instance.column_names
