# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Simple utility class that represents tables with geometry.
"""

from . import Table
from .mixins import GeoDataMixin


class GeoTable(GeoDataMixin, Table):
    """
        Provides an interface to get a representation of a table
        with geographic information.

        Parameters
        ----------
        name : str
            Name of the table, may be fully qualified
        schema : str, optional, default None
            Optional if name is fully qualified
        columns : str, optional, default None
            Optional list of columns
        geom_column : str, optional, default "geom"
            Name of the column containing geometry
        gid_column : str, optional, default None
            Name of the column containing a gid, if set to None
            gid will be row numbers unless a gid column is present.

        Examples
        --------
        >>> t = GeoTable(name="admin3", schema="geography")
        >>> t.to_geojson()['features'][0]
         ..
          'type': 'MultiPolygon'},
         'id': 1,
         'properties': {'admin0name': 'Nepal',
          'admin0pcod': 'NP',
          'admin1name': 'Central Development Region',
          'admin1pcod': '524 2',
          'admin2name': 'Bagmati',
          'admin2pcod': '524 2 05',

        """

    def __init__(
        self, name=None, schema=None, columns=None, geom_column="geom", gid_column=None
    ):
        self.geom_column = geom_column
        self.gid_column = gid_column
        super().__init__(name=name, schema=schema, columns=columns)
        if geom_column not in self.column_names:
            raise ValueError(
                "geom_column: {} is not a column in this table.".format(geom_column)
            )
        if gid_column is not None and gid_column not in self.column_names:
            raise ValueError(
                "gid_column: {} is not a column in this table.".format(gid_column)
            )

    def _geo_augmented_query(self):
        if self.gid_column is None:
            if "gid" in self.column_names:
                gid_column = "gid"
            else:
                gid_column = "row_number() over()"
        else:
            gid_column = self.gid_column
        cols = [c for c in self.column_names if c not in (gid_column, self.geom_column)]
        aliased_cols = cols + [f"{gid_column} as gid", f"{self.geom_column} as geom"]
        return (
            f"SELECT {', '.join(aliased_cols)} FROM ({self.get_query()}) as t",
            cols + ["gid", "geom"],
        )
