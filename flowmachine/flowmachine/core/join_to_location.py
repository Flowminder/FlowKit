# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Core class definition JoinToLocation, which facilitates
the joining of any query with cell/site information to
another spatial level, such as a grid or an admin region.

"""
from typing import List

from .query import Query
from .custom_query import CustomQuery
from .spatial_unit import SpatialUnit


class JoinToLocation(Query):
    """
    Intermediate class which joins any query object, or python
    string representing a query object, to some geographical level.
    This can be simply the site with a version a lat-lon value, an
    admin region, a gridded map, or any arbitrary polygon. This
    will return everything in the original query, plus an additional 
    column or columns representing the spatial region that the infrastructure
    element lies within.

    Parameters
    ----------
    left : str or flowmachine.Query
        String to table (with the schema) or else a flowmachine.Query object.
        This represents a table that can be joined to the cell information
        table. This must have a date column (called time) and a location column
        call 'location_id'.
    spatial_unit : flowmachine.SpatialUnit
        A query which maps cell identifiers in the CDR to a different spatial
        unit (e.g. versioned site or admin region)
    time_col : str, default 'time':
        The name of the column that identifies the time in the source table
        e.g. 'time', 'date', 'start_time' etc.

    See Also
    --------

    flowmachine/docs/source/notebooks/working_with_locations.ipynb

    for further examples of how this works.

    """

    def __init__(self, left, *, spatial_unit, time_col="time"):
        if not isinstance(spatial_unit, SpatialUnit):
            raise TypeError("spatial_unit must be a SpatialUnit object")
        self.spatial_unit = spatial_unit
        # If the user passes a string, rather than a flowmachine.Query object
        # then we'll simply turn this string into a flowmachine.Query object
        # and proceed as normal.
        if type(left) is str:
            self.left = CustomQuery(left, left.column_names)
        else:
            self.left = left
        self.time_col = time_col
        super().__init__()

    def __getattr__(self, name):
        # Don't extend this to hidden variables, such as _df
        # and _len
        if name.startswith("_"):
            raise AttributeError
        try:
            return self.left.__getattribute__(name)
        except AttributeError:
            return self.spatial_unit.__getattribute__(name)

    @property
    def column_names(self) -> List[str]:
        right_columns = self.spatial_unit.location_columns
        left_columns = self.left.column_names
        if "location_id" in right_columns and "location_id" in left_columns:
            left_columns.remove("location_id")
        return left_columns + right_columns

    def _make_query(self):
        right_columns = self.spatial_unit.location_columns
        left_columns = self.left.column_names
        if "location_id" in right_columns and "location_id" in left_columns:
            left_columns.remove("location_id")

        right_columns_str = ", ".join([f"sites.{c}" for c in right_columns])
        left_columns_str = ", ".join([f"l.{c}" for c in left_columns])

        sql = f"""
        SELECT
            {left_columns_str},
            {right_columns_str}
        FROM
            ({self.left.get_query()}) AS l
        INNER JOIN
            ({self.spatial_unit.get_query()}) AS sites
        ON
            l.location_id = sites.location_id
          AND
            l.{self.time_col}::date BETWEEN coalesce(sites.date_of_first_service,
                                                '-infinity'::timestamptz) AND
                                       coalesce(sites.date_of_last_service,
                                                'infinity'::timestamptz)
        """

        return sql
