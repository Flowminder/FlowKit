# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
from typing import List, Optional, Tuple
from typing import Union as UnionType

from flowmachine.core import Query
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.core.union import Union
from flowmachine.features.subscriber.total_active_periods import (
    TotalActivePeriodsSubscriber,
)
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.utilities.unique_values_from_queries import (
    UniqueValuesFromQueries,
)
from flowmachine.utils import time_period_add, standardise_date


class TotalLocatablePeriods(TotalActivePeriodsSubscriber):
    """
    Breaks a time span into distinct time periods (integer number of days, hours or minutes).
    For each subscriber counts the total number of time periods in
    which each subscriber was seen at locatable cells.
    For instance we might ask for a month worth of data, break down our
    month into 10 3 day chunks, and ask for each subscriber how many of these
    three day chunks each subscriber was present and locatable in the data in.

    This is equivalent to TotalActivePeriodsSubscriber, except that this query
    only considers events from cells that map to a location (using the specified spatial unit).

    Note: "locatable cell" here means a cell whose ID appears in the "location_id" column
    of the specified spatial unit, even if this location_id is mapped to a null location.

    Parameters
    ----------
    start : str
        iso-format date, start of the analysis.
    total_periods : int
        Total number of periods to break your time span into
    period_length : int, default 1
        Total number of days per period.
    period_unit : {'days', 'hours', 'minutes'} default 'days'
        Split this time frame into hours or days etc.
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default versioned-cell
        Spatial unit defining the set of "locatable" cell IDs.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    periods_to_exclude : list of str
        List of datetimes corresponding to the starts of periods that should be excluded
        (e.g. to ignore a date in the middle of the window)

    See Also
    --------
    flowmachine.features.subscriber.total_active_periods.TotalActivePeriodsSubscriber
    """

    def __init__(
        self,
        *,
        start: str,
        total_periods: int,
        period_length: int = 1,
        period_unit: str = "days",
        spatial_unit: AnySpatialUnit,
        hours: Optional[Tuple[int, int]] = None,
        table: Optional[UnionType[str, List[str]]] = None,
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
        periods_to_exclude: Optional[List[str]] = None,
    ):
        self.spatial_unit = spatial_unit
        if periods_to_exclude is not None:
            # Filter periods to exclude to only relevant dates
            # (otherwise changes to irrelevant dates will change query ID)
            lower_bound = standardise_date(start)
            upper_bound = time_period_add(
                start, total_periods * period_length, period_unit
            )
            # Needs to be sorted so that the query ID is deterministic
            self.periods_to_exclude = sorted(
                set(
                    standardise_date(p)
                    for p in periods_to_exclude
                    if (
                        standardise_date(p) >= lower_bound
                        and standardise_date(p) < upper_bound
                    )
                )
            )
        else:
            self.periods_to_exclude = []
        super().__init__(
            start=start,
            total_periods=total_periods,
            period_length=period_length,
            period_unit=period_unit,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )

    def _get_start_stops(self):
        """
        Gets two lists, one for the start dates and one for the
        stop dates.
        """

        starts = [
            time_period_add(self.start, i * self.period_length, self.period_unit)
            for i in range(self.total_periods)
        ]
        stops = [
            time_period_add(self.start, (i + 1) * self.period_length, self.period_unit)
            for i in range(self.total_periods)
        ]
        filtered_startstops = [
            (start, stop)
            for start, stop in zip(starts, stops)
            if start not in self.periods_to_exclude
        ]
        if not filtered_startstops:
            raise ValueError(
                "Cannot create a TotalLocatablePeriods query with all periods excluded"
            )
        filtered_starts, filtered_stops = zip(
            *(
                (start, stop)
                for start, stop in zip(starts, stops)
                if start not in self.periods_to_exclude
            )
        )
        return filtered_starts, filtered_stops

    def _get_unioned_subscribers_list(
        self,
        hours: UnionType[str, Tuple[int, int]] = "all",
        table: Optional[UnionType[str, List[str]]] = None,
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
    ):
        """
        Constructs a list of unique locatable subscribers for each time period.
        They will be unique within each time period. Joins
        these lists into one long list and returns the result
        (as a query)
        """
        # Union doesn't handle the case where there's only one query in the list,
        # so handle that separately here
        if len(self.starts) == 1:
            return UniqueValuesFromQueries(
                query_list=SubscriberLocations(
                    self.starts[0],
                    self.stops[0],
                    spatial_unit=self.spatial_unit,
                    hours=hours,
                    table=table,
                    subscriber_identifier=subscriber_identifier,
                    subscriber_subset=subscriber_subset,
                ),
                column_names="subscriber",
            )
        else:
            return Union(
                *[
                    UniqueValuesFromQueries(
                        query_list=SubscriberLocations(
                            start,
                            stop,
                            spatial_unit=self.spatial_unit,
                            hours=hours,
                            table=table,
                            subscriber_identifier=subscriber_identifier,
                            subscriber_subset=subscriber_subset,
                        ),
                        column_names="subscriber",
                    )
                    for start, stop in zip(self.starts, self.stops)
                ],
                all=True,
            )
