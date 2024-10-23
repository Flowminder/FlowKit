# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that deal with finding a list of locations for subscribers.
Note these classes return multiple locations for each subscriber,
and therefore do not represent a single home location for each
subscribers. Mostly they are not used directly, but are called by
dailylocations objects, although they can be.

"""


from flowmachine.utils import (
    parse_datestring,
    standardise_date,
    standardise_date_to_datetime,
)

from ...core import CustomQuery

import structlog

from ...core.union_with_fixed_values import UnionWithFixedValues

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class MultiLocation:
    """
    Abstract base class for any class that involves stitching together
    multiple daily locations (or similar).

    The class object takes a start and stop datetime, and optionally
    a list of daily locations objects and returns a day-dated list of locations of
    each subscriber. This will be the first location in the event of a tie.
    Subscribers are guaranteed to have a Day Trajectory if they appear in any of
    the daily_locs objects.

    Parameters
    ----------
    daily_locations : list, optional list of flowmachine.daily_location objects
            to use for calculation.
    """

    def __init__(self, *daily_locations):
        # TODO: check that all the inputs are actually location objects (of an appropriate kind)

        self.start = standardise_date(
            min(
                parse_datestring(daily_location.start)
                for daily_location in daily_locations
            )
        )
        self.stop = standardise_date(
            max(
                parse_datestring(daily_location.start)
                for daily_location in daily_locations
            )
        )
        self.unioned = UnionWithFixedValues(
            queries=daily_locations,
            fixed_value_column_name="date",
            fixed_value=[
                standardise_date_to_datetime(dl.start) for dl in daily_locations
            ],
        )
        logger.info(
            "ModalLocation using {} DailyLocations".format(len(daily_locations))
        )
        logger.info(
            "{}/{} DailyLocations are pre-calculated.".format(
                sum(1 for dl in daily_locations if dl.is_stored), len(daily_locations)
            )
        )

        # Importing daily_location inputs
        # from first daily_location object.
        self.spatial_unit = daily_locations[0].spatial_unit
        self.subscriber_identifier = daily_locations[0].subscriber_identifier
        super().__init__()
