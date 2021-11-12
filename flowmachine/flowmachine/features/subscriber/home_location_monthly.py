from typing import List, Union
from flowmachine.core.spatial_unit import AnySpatialUnit
from datetime import datetime, timedelta
from flowmachine.core.query import Query
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY
from copy import copy

from flowmachine.features.subscriber.modal_location import ModalLocation
from flowmachine.features.subscriber.location_visits import LocationVisits
from flowmachine.core.errors import InvalidSpatialUnitError


class MajorityLocation(Query):

    """For each subscriber, if their modal last location was their last location on more than X days this month
    (choose X=15 here), then choose this as their home location. If not, but it was their last location on more than Y
     days this month (Y<X; choose Y=10 here) AND it is the same as their “reference location” (in our case reference
     location is modal location and last location on more than X days last month), then choose this as their home
     location. Otherwise set their home location to “unlocatable” (a special value, not NULL)."""

    def __init__(
        self,
        location_visits: LocationVisits,
        subscriber_subset: Query,
        reference_location: Union["MajorityLocation", None] = None,
        home_last_month: Union[int, None] = None,
    ):
        self.subscriber_subset = subscriber_subset
        self.location_visits = location_visits
        self.reference_location = reference_location

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        cols = ["subscriber", "location", "month"]
        return cols

    def _make_query(self):

        loc_col = self.spatial_unit.location_column

        if self.reference_location:
            ref_clause_1 = f"""OR ( modal_locations.loc_col = reference_locations.{loc_col}
                               AND location_histogram.y >= {self.home_last_month}"""
            ref_clause_2 = """LEFT JOIN reference_locations USING (subscriber) """
            ref_clause_3 = f"""OR ( modal_locations.{loc_col} = reference_locations.{loc_col}
                               AND location_histogram.y >= {self.home_last_month} )"""
        else:
            ref_clause_1, ref_clause_2, ref_clause_3 = [""] * 3

        sql = f"""
WITH last_locations AS (
    {self.location_visits.get_query()}
), modal_locations AS (
    {self.modal_locations.get_query()}
), location_histogram AS (
    {self.location_visits.get_query()}
)
SELECT modal_locations.subscriber,
    CASE WHEN ( location_histogram.y >= {self.home_this_month} ) 
    {ref_clause_1}
    THEN modal_locations.{loc_col} ELSE 'unknown' END AS pcod
modal_locations.pcod
FROM modal_locations 
INNER JOIN location_histogram 
USING ( subscriber, pcod ) 
{ref_clause_2}
WHERE location_histogram.y >= {self.home_this_month} 
{ref_clause_3}
"""
        return sql
