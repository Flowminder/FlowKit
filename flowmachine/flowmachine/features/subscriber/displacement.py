# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""

The maximum displacement of a user from its home location



"""
from typing import List

from flowmachine.features.subscriber import daily_location
from .metaclasses import SubscriberFeature
from . import ModalLocation
from ..utilities.subscriber_locations import SubscriberLocations, BaseLocation
from flowmachine.utils import parse_datestring, get_dist_query_string, list_of_dates
from flowmachine.core import make_spatial_unit

from dateutil.relativedelta import relativedelta

valid_stats = {"sum", "avg", "max", "min", "median", "stddev", "variance"}


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
    statistic : str
        the statistic to calculate one of 'sum', 'avg', 'max', 'min', 
        'median', 'stddev' or 'variance'
    unit : {'km', 'm'}, default 'km'
        Unit with which to express the answers, currently the choices
        are kilometres ('km') or metres ('m')
    
    Other parameters
    ----------------
    hours : tuple of ints, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    method : str, default 'last'
        The method by which to calculate the location of the subscriber.
        This can be either 'most-common' or last. 'most-common' is
        simply the modal location of the subscribers, whereas 'lsat' is
        the location of the subscriber at the time of the final call in
        the data.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'ALL' it will use all tables that contain
        location data, specified in flowmachine.yml.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
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
        start,
        stop,
        reference_location,
        statistic="avg",
        unit="km",
        hours="all",
        method="last",
        table="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        subscriber_subset=None,
    ):

        # need to subtract one day from hl end in order to be
        # comparing over same period...
        self.stop_sl = stop
        self.stop_hl = str(parse_datestring(stop) - relativedelta(days=1))

        self.start = start

        sl = SubscriberLocations(
            self.start,
            self.stop_sl,
            spatial_unit=make_spatial_unit("lon-lat"),
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        if isinstance(reference_location, BaseLocation):

            self.joined = reference_location.join(
                sl,
                on_left="subscriber",
                on_right="subscriber",
                how="left",
                left_append="_home_loc",
                right_append="",
            )
            reference_location.spatial_unit.verify_criterion("has_lon_lat_columns")
        else:
            raise ValueError(
                "Argument 'reference_location' should be an instance of BaseLocation class. "
                f"Got: {type(reference_location)}"
            )

        self.unit = unit

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):

        dist_string = get_dist_query_string(
            lon1="lon_home_loc", lat1="lat_home_loc", lon2="lon", lat2="lat"
        )

        if self.unit == "km":
            divisor = 1000
        elif self.unit == "m":
            divisor = 1

        sql = """
        select 
            subscriber,
            {statistic}({dist_string}) / {divisor} as value
        from 
            ({join}) as foo
        group by 
            subscriber
        """.format(
            statistic=self.statistic,
            dist_string=dist_string,
            divisor=divisor,
            join=self.joined.get_query(),
        )

        return sql
