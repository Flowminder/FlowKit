# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union, Optional, Tuple

from ..utilities.direction_enum import Direction

"""
Location introversion [1]_ calculates the proportion
of interactions made within a certain location 
in which the interaction counterpart is located
in the same location. Its opposite is
measured by location extroversion.


References
---------
.. [1] Christopher Smith, Afra Mashhadi, Licia Capra. "Ubiquitous Sensing for Mapping Poverty in Developing Countries". NetMob Conference Proceedings, 2013. http://haig.cs.ucl.ac.uk/staff/L.Capra/publicatiONs/d4d.pdf

"""
from flowmachine.core.query import Query
from flowmachine.core.mixins.geodata_mixin import GeoDataMixin


from flowmachine.core.join_to_location import location_joined_query
from flowmachine.core.spatial_unit import AnySpatialUnit, make_spatial_unit
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.utils import make_where, standardise_date


class LocationIntroversion(GeoDataMixin, Query):
    """
    Calculates the proportions of events that take place
    within a location in which all involved parties
    are located in the same location (introversion), and those
    which are between parties in different locations (extroversion).

    Parameters
    ----------
    start : str
        ISO format date string to at which to start the analysis
    stop : str
        AS above for the end of the analysis
    table : str, default 'all'
        Specifies a table of cdr data on which to base the analysis. Table must
        exist in events schema. If 'ALL' then we use all tables specified in
        flowmachine.yml.
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH.
        Determines if query should filter only outgoing
        events ('out'), incoming events ('in'), or both ('both').

    Notes
    -----

    Equation 3 of the original paper states introversion as the ratio of introverted to extroverted events
    but indicates that this will return values in the range [0, 1]. However, the preceding text indicate
    that introversion is the _proportion_ of events which are introverted. We follow the latter here.

    Examples
    --------
    >>> LocationIntroversion("2016-01-01", "2016-01-07").head()
          location_id  introversion  extroversion
    0    AUQZGMW3      0.050000      0.950000
    1    ns6vzdkC      0.049180      0.950820
    2    llTlNC7E      0.046122      0.953878
    3    WET2L101      0.045549      0.954451
    4    eAwMUT94      0.045175      0.954825
    """

    def __init__(
        self,
        start: str,
        stop: str,
        *,
        table: str = "all",
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        direction: Union[Direction, str] = Direction.BOTH,
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.table = table
        self.spatial_unit = spatial_unit
        self.direction = Direction(direction)

        self.unioned_query = location_joined_query(
            EventsTablesUnion(
                self.start,
                self.stop,
                columns=[
                    "id",
                    "outgoing",
                    "location_id",
                    "datetime",
                    subscriber_identifier,
                ],
                tables=self.table,
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=subscriber_identifier,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        location_columns = self.spatial_unit.location_id_columns
        sql = f"""
        WITH unioned_table AS ({self.unioned_query.get_query()})
        SELECT {', '.join(location_columns)}, sum(introverted::integer)/count(*)::float as value FROM (
            SELECT
               {', '.join(f'A.{c} as {c}' for c in location_columns)},
               {' AND '.join(f'A.{c} = B.{c}' for c in location_columns)} as introverted
            FROM unioned_table as A
            INNER JOIN unioned_table AS B
                  ON A.id = B.id
                     AND A.outgoing != B.outgoing
                     {make_where(self.direction.get_filter_clause(prefix="A"))}
        ) _
        GROUP BY {', '.join(location_columns)}
        ORDER BY {', '.join(location_columns)}
        """

        return sql
