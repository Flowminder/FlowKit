# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Optional
from flowmachine.core import Query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.utils import standardise_date


class VisitedMostDays(Query):
    """
    A query that returns the number of subscribers who visited a place
    on most days.
    """

    def __init__(
        self, start_date, end_date, spatial_unit: Optional[AnySpatialUnit] = None
    ):
        self.start_date = standardise_date(start_date)
        self.end_date = standardise_date(end_date)
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=1)
        else:
            self.spatial_unit = spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        return f"""
        SELECT 
        'ABC_1.2.3' as {self.column_names[0]},
        42 AS value
        """
