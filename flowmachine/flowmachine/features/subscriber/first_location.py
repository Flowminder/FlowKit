# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Identifies the first location in which a subscriber
is seen within a specified time period.



"""
from typing import List

from flowmachine.core import make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import SubscriberLocations
from flowmachine.utils import standardise_date


class FirstLocation(SubscriberFeature):
    """
    Class that defines the time at which the subscriber was first spotted
    at the location in question. Note this does not imply that the
    subscriber was never here before, but this is the first time within
    the specified time range.

    Parameters
    ----------
    start : str
        String representing the beginning of the focal time period
    stop : str
        String representing the end of the focal period
    location : str, dict, tuple, or list/tuple thereof
        str representing the location of interest. Could be
        a cell or an admin region for instance. You must specify
        spatial_unit to match this. i.e. location='ER0980',
        spatial_unit=make_spatial_unit('cell').
        Can also pass a list of strings e.g. ['ER0980', 'CW2020']
        will return the time at which the subscriber was first at any
        of these locations. Pass the argument 'any', to find the
        first time a subscriber pops up at any location.
        For spatial units with multiple location_id_columns, see
        `SpatialUnitMixin.location_subset_clause` or
        `LonLatSpatialUnit.location_subset_clause` for a description of the
        allowed formats for the location argument.
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    See Also
    --------
    flowmachine.features.subscriber_locations
    """

    def __init__(
        self,
        start,
        stop,
        *,
        location,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        subscriber_subset=None,
    ):
        """"""

        if location == "any" and spatial_unit != make_spatial_unit("cell"):
            raise ValueError(
                "Invalid parameter combination: location='any' can only be used with cell spatial unit."
            )

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.location = location

        self.ul = SubscriberLocations(
            self.start,
            self.stop,
            spatial_unit=spatial_unit,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )

        self.table = self.ul.table
        self.subscriber_identifier = self.ul.subscriber_identifier

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "time"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        clause = self._get_locations_clause(self.location)

        sql = f"""
        SELECT 
            relevant_locs.subscriber,
            min(time) AS time
        FROM
            (SELECT * FROM ({self.ul.get_query()}) AS subscriber_locs
            {clause}) AS relevant_locs
        GROUP BY relevant_locs.subscriber
        """

        return sql

    def _get_locations_clause(self, location):
        """
        Private method for getting location clause
        in statement.
        """
        if location == "any":
            return ""
        else:
            return self.ul.spatial_unit.location_subset_clause(self.location)
