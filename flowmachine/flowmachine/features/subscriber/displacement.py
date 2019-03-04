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
from ..utilities.subscriber_locations import subscriber_locations
from flowmachine.utils import parse_datestring, get_dist_string, list_of_dates

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
    modal_locations : ModalLocation
        The set of home locations from which to calculate displacement.
        If not given then ModalLocation Query wil be created over period
        start -> stop.
    statistic : str
        the statistic to calculate one of 'sum', 'avg', 'max', 'min', 
        'median', 'stddev' or 'variance'
    unit : {'km', 'm'}, default 'km'
        Unit with which to express the answers, currently the choices
        are kilometres ('km') or metres ('m')
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

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
        self, start, stop, modal_locations=None, statistic="avg", unit="km", **kwargs
    ):

        # need to subtract one day from hl end in order to be
        # comparing over same period...
        self.stop_sl = stop
        self.stop_hl = str(parse_datestring(stop) - relativedelta(days=1))

        self.start = start

        allowed_levels = ["lat-lon", "versioned-cell", "versioned-site"]
        if modal_locations:
            if (
                isinstance(modal_locations, ModalLocation)
                and modal_locations.level in allowed_levels
            ):
                hl = modal_locations
            else:
                raise ValueError(
                    f"Argument 'modal_locations' should be an instance of ModalLocation class with level in {allowed_levels}"
                )
        else:
            hl = ModalLocation(
                *[
                    daily_location(date, level="lat-lon", **kwargs)
                    for date in list_of_dates(self.start, self.stop_hl)
                ]
            )

        sl = subscriber_locations(self.start, self.stop_sl, level="lat-lon", **kwargs)

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        self.joined = hl.join(
            sl,
            on_left="subscriber",
            on_right="subscriber",
            how="left",
            left_append="_home_loc",
            right_append="",
        )

        self.unit = unit

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "statistic"]

    def _make_query(self):

        dist_string = get_dist_string("lat_home_loc", "lon_home_loc", "lat", "lon")

        if self.unit == "km":
            divisor = 1000
        elif self.unit == "m":
            divisor = 1

        sql = """
        select 
            subscriber,
            {statistic}({dist_string}) / {divisor} as statistic
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
