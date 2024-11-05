# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
The maximum displacement of a user from its home location
"""
from typing import List, Union, Tuple, Optional

from flowmachine.features.spatial import DistanceMatrix
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import SubscriberLocations, BaseLocation
from flowmachine.core import Query
from flowmachine.utils import standardise_date
from ...core.statistic_types import Statistic


class Displacement(SubscriberFeature):
    """
    Calculates  statistics of a subscribers displacement from
    their home location.

    Class representing the displacement from their home location
    for all subscribers within a certain time frame. This will
    return displacement from home for all subscribers in ModalLocation.
    If a user with a home location makes no calls between start and stop
    then a NaN value will be returned.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    reference_location : BaseLocation
        The set of home locations from which to calculate displacement.
        If not given then ModalLocation Query wil be created over period
        start -> stop.
    statistic : Statistic, default Statistic.AVG
        The statistic to calculate.
    unit : {'km', 'm'}, default 'km'
        Unit with which to express the answers, currently the choices
        are kilometres ('km') or metres ('m')
    hours : tuple of ints, default None
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'all' it will use all tables that contain
        location data.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    return_subscribers_not_seen : bool, default False
        By default, subscribers who are not seen in the time period are dropped. Set to True
        to return them as NULL.
    ignore_nulls : bool, default True
        ignores those values that are null. Sometime data appears for which
        the cell is null. If set to true this will ignore those lines. If false
        these lines with null cells should still be present, although they contain
        no information on the subscribers location, they still tell us that the subscriber made
        a call at that time.

    Examples
    --------
    >>> d = Displacement('2016-01-01 13:30:30', '2016-01-02 16:25:00')
    >>> d.head()
    subscriber   avg_displacement
    subscriberA  13.0
    subscriberB  12.3
    subscriberC   6.5
    """

    def __init__(
        self,
        start: str,
        stop: str,
        reference_location: BaseLocation,
        statistic: Statistic = Statistic.AVG,
        unit: str = "km",
        hours: Union[str, Tuple[int, int]] = "all",
        table: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        ignore_nulls: bool = True,
        return_subscribers_not_seen: bool = False,
        subscriber_subset: Optional[Query] = None,
    ):
        self.return_subscribers_not_seen = return_subscribers_not_seen
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.spatial_unit = reference_location.spatial_unit
        subscriber_locations = SubscriberLocations(
            self.start,
            self.stop,
            spatial_unit=self.spatial_unit,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )

        self.statistic = Statistic(statistic.lower())

        if not isinstance(reference_location, BaseLocation):
            raise ValueError(
                "Argument 'reference_location' should be an instance of BaseLocation class. "
                f"Got: {type(reference_location)}"
            )
        else:
            self.reference_location = reference_location
            self.joined = reference_location.join(
                other=subscriber_locations,
                on_left=["subscriber"],
                left_append="_from",
                right_append="_to",
            ).join(
                DistanceMatrix(spatial_unit=self.spatial_unit),
                on_left=[
                    f"{col}_{direction}"
                    for direction in ("from", "to")
                    for col in self.spatial_unit.location_id_columns
                ],
                right_append="_dist",
                how="left outer",
            )

        self.unit = unit

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        if self.unit == "m":
            multiplier = 1000
        elif self.unit == "km":
            multiplier = 1

        sql = f"""
        SELECT 
            subscriber,
            {self.statistic:COALESCE(value_dist, 0) * {multiplier}} as value
        FROM 
            ({self.joined.get_query()}) _
        GROUP BY 
            subscriber
        """

        if self.return_subscribers_not_seen:  # Join back onto the reference location
            sql = f"""
            SELECT subscriber, dists.value
            FROM
            ({self.reference_location.get_query()}) _
            LEFT OUTER JOIN
            ({sql}) dists
            USING (subscriber)
            """

        return sql
