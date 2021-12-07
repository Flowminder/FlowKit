# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin


class SpatialAggregate(GeoDataMixin, Query):
    """
    Class representing the result of spatially aggregating
    a locations object. A locations object represents the
    location of multiple subscribers. This class represents the output
    of aggregating that data spatially.

    Parameters
    ----------
    locations : subscriber location query
    subscriber_label: A query with a  column to further disaggregate by
    label_column: Name of the column containing the label: defaults to 'value'
    """

    def __init__(self, *, locations, subscriber_labels=None, label_column="value"):

        self.locations = locations
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = locations.spatial_unit
        self.subscriber_labels = subscriber_labels
        self.label_column = label_column
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        # Do we want to return the labels column as well?
        return self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):

        aggregate_cols = ",".join(self.spatial_unit.location_id_columns)

        if self.subscriber_labels:
            label_clause = f"JOIN ({self.subscriber_labels.get_query()}) AS labels USING (subscriber)"
            label_select_clause = f", labels.{self.label_column} AS label"
            label_agg_clause = f", label"
        else:
            label_clause = ""
            label_select_clause = ""
            label_agg_clause = ""

        sql = f"""
        SELECT
            {aggregate_cols}{label_select_clause},
            count(*) AS value
        FROM
            ({self.locations.get_query()})  AS to_agg {label_clause}
        GROUP BY
            {aggregate_cols}{label_agg_clause}
        """

        return sql
