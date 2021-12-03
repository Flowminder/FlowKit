# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.features.subscriber.metaclasses import SubscriberFeature


class MobilityClassification(SubscriberFeature):
    def __init__(self, locations, stay_length_threshold):
        self.locations = locations
        if len(set(l.spatial_unit for l in self.locations)) > 1:
            raise ValueError(
                "MobilityClassification requires all input locations to have the same spatial unit"
            )
        self.stay_length_threshold = int(stay_length_threshold)
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        loc_cols_string = ", ".join(self.locations[0].spatial_unit.location_id_columns)
        locations_union = " UNION ALL ".join(
            f"SELECT subscriber, {loc_cols_string}, {i} AS ordinal FROM ({loc.get_query()}) _"
            for i, loc in enumerate(self.locations)
        )

        long_term_activity = f"""
        SELECT
            subscriber,
            count(*) < {len(self.locations)} AS sometimes_inactive,
            count(coalesce({loc_cols_string})) < {len(self.locations)} AS sometimes_unlocatable
        FROM ({locations_union}) AS locations_union
        GROUP BY subscriber
        """

        # Find stay lengths using gaps-and-islands approach
        long_term_mobility = f"""
        SELECT subscriber, max(stay_length) AS longest_stay
        FROM (
            SELECT subscriber, count(*) AS stay_length
            FROM (
                SELECT
                    subscriber,
                    {loc_cols_string},
                    ordinal - dense_rank() OVER (
                        PARTITION BY subscriber, {loc_cols_string}
                        ORDER BY ordinal
                    ) AS stay_id
                FROM ({locations_union}) AS locations_union
            ) locations_with_stay_id
            GROUP BY subscriber, {loc_cols_string}, stay_id
        ) stay_lengths
        GROUP BY subscriber
        """

        sql = f"""
        SELECT
            subscriber,
            CASE
                WHEN coalesce({loc_cols_string}) IS NULL THEN 'unlocated'
                WHEN sometimes_inactive THEN 'irregular'
                WHEN sometimes_unlocatable THEN 'not_always_locatable'
                WHEN longest_stay < {self.stay_length_threshold} THEN 'mobile'
                ELSE 'stable'
            END AS value
        FROM ({self.locations[-1].get_query()}) AS most_recent_period
        LEFT JOIN ({long_term_activity}) AS long_term_activity
        USING (subscriber)
        LEFT JOIN ({long_term_mobility}) AS long_term_mobility
        USING (subscriber)
        """

        return sql
