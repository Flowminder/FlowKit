# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the radius of gyration for subscribers
within a specified time period. Radius of gyration
can be calculated in `km` or `m`.


"""
from typing import List

from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import subscriber_locations


class RadiusOfGyration(SubscriberFeature):
    """
    Calculates the radius of gyration.

    Class representing the radius of gyration for all subscribers
    within a certain time frame.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    unit : str, default 'km'
        Unit with which to express the answers, currently the choices
        are kilometres ('km') or metres ('m')
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    args and kwargs passed to flowmachine.subscriber_locations

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    Examples
    --------
    >>> RoG = RadiusOfGyration('2016-01-01 13:30:30',
                               '2016-01-02 16:25:00')
    >>> RoG.head()
     subscriber   RoG
    subscriberA  13.0
    subscriberB  12.3
    subscriberC   6.5
    ...

    """

    def __init__(self, start, stop, unit="km", *args, **kwargs):
        """
        """

        allowed_units = ["km", "m"]
        if unit not in allowed_units:
            raise ValueError(
                "Unrecognised unit {},".format(unit)
                + " use one of {}".format(allowed_units)
            )

        self.start = start
        self.stop = stop
        self.unit = unit
        self.ul = subscriber_locations(
            self.start, self.stop, level="lat-lon", *args, **kwargs
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        av_dist = f"""
        SELECT 
            subscriber_locs.subscriber, 
            avg(lat) AS av_lat, 
            avg(lon) AS av_long
        FROM ({self.ul.get_query()}) AS subscriber_locs
        GROUP BY subscriber_locs.subscriber
        """

        distance_string = """
        ST_Distance(ST_Point(locs.lon, locs.lat)::geography,
                    ST_point(mean.av_long, mean.av_lat)::geography)
        """

        # It seems like I'm creating the sub query twice here
        # I wonder whether this slows things down at all.
        dist = f"""
        SELECT
            locs.subscriber,
            ({distance_string})^2
            AS distance_sqr
        FROM
            ({self.ul.get_query()}) AS locs
        INNER JOIN
            ({av_dist}) AS mean
            ON locs.subscriber=mean.subscriber
        """

        if self.unit == "km":
            divisor = 1000
        elif self.unit == "m":
            divisor = 1

        return f"""
        SELECT
            dist.subscriber,
            sqrt( avg( distance_sqr ) )/{divisor} AS value
        FROM 
            ({dist}) AS dist
        GROUP BY dist.subscriber
        """
