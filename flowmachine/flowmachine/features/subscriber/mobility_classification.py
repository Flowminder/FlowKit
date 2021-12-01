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
        # Can probably skip this sub-query, and just join to locations[-1] and put "coalesce({loc_cols_string}) IS NULL" in the final step
        this_month_activity = f"""
        SELECT
            subscriber,
            coalesce({loc_cols_string}) IS NULL AS unlocatable
        FROM ({self.locations[-1].get_query()}) AS most_recent_period
        """

        long_term_activity = f"""
        SELECT
            subscriber,
            count(*) < {len(self.locations)} AS sometimes_inactive,
            count(coalesce({loc_cols_string})) < {len(self.locations)} AS sometimes_unlocatable
        FROM ({locations_union}) locations_union
        GROUP BY subscriber
        """

        # Alternative approach for long_term_activity
        # Note: could equally well use INTERSECT ALL or INNER JOIN here
        long_term_active = " INTERSECT ".join(
            f"SELECT subscriber FROM ({loc.get_query()}) _" for loc in self.locations
        )
        long_term_locatable = " INTERSECT ".join(
            f"SELECT subscriber FROM ({loc.get_query()}) _ WHERE coalesce({loc_cols_string}) IS NOT NULL"
            for loc in self.locations
        )
        alternative_long_term_activity = f"""
        SELECT subscriber, TRUE AS always_active, always_locatable
        FROM ({long_term_active}) long_term_active
        LEFT JOIN (
            SELECT subscriber, TRUE AS always_locatable
            FROM ({long_term_locatable}) _
        ) long_term_locatable
        USING (subscriber)
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
                FROM ({locations_union}) locations_union
            ) locations_with_stay_id
            GROUP BY subscriber, {loc_cols_string}, stay_id
        )
        GROUP BY subscriber
        """

        # TODO: try to choose some shorter labels
        sql = f"""
        SELECT
            subscriber,
            CASE
                WHEN coalesce({loc_cols_string}) IS NULL THEN 'unlocatable'
                WHEN sometimes_inactive THEN 'sometimes_inactive'
                WHEN sometimes_unlocatable THEN 'sometimes_unlocatable'
                WHEN longest_stay < {self.stay_length_threshold} THEN 'highly_mobile'
                ELSE 'stable'
            END AS value
        FROM ({self.locations[-1].get_query()}) AS most_recent_period
        LEFT JOIN ({long_term_activity}) AS long_term_activity
        USING (subscriber)
        LEFT JOIN ({long_term_mobility}) AS long_term_mobility
        USING (subscriber)
        """

        # Alternative, using alternative_long_term_activity
        alternative_sql = f"""
        SELECT
            subscriber,
            CASE
                WHEN coalesce({loc_cols_string}) IS NULL THEN 'unlocatable'
                WHEN NOT coalesce(always_active, FALSE) THEN 'sometimes_inactive'
                WHEN NOT coalesce(always_locatable, FALSE) THEN 'sometimes_unlocatable'
                WHEN longest_stay < {self.stay_length_threshold} THEN 'highly_mobile'
                ELSE 'stable'
            END AS value
        FROM ({self.locations[-1].get_query()}) AS most_recent_period
        LEFT JOIN ({long_term_activity}) AS alternative_long_term_activity
        USING (subscriber)
        LEFT JOIN ({long_term_mobility}) AS long_term_mobility
        USING (subscriber)
        """

        return sql


# Approach 1 - assign boolean flags to all subscribers, then join and get label using CASE WHEN:
#     1. Identify subscribers as "unlocatable" or not this month
#     2. Identify subscribers as "sometimes_inactive" or not, and "sometimes_unlocatable" or not, over all months
#     3. Identify subscribers as "highly_mobile" or not over all months
#     4. SELECT subscriber, unlocatable, sometimes_inactive, sometimes_unlocatable, highly_mobile
#        FROM (1) this_month_activity
#        LEFT JOIN (2) long_term_activity
#        LEFT JOIN (3) long_term_mobility
#        USING (subscriber)
#     5. CASE WHEN unlocatable THEN 'unlocatable'
#             WHEN sometimes_inactive THEN 'sometimes_inactive'
#             WHEN sometimes_unlocatable THEN 'sometimes_unlocatable'
#             WHEN highly_mobile THEN 'highly_mobile'
#             ELSE 'stable'
#
# Approach 2 - get subscriber subset for each level in flow diagram, then recursively join and get label using COALESCE:
#     1. All subs this month ("active")
#     2. All subs this month where location is not null ("locatable")
#     3. (2) INTERSECT (intersection of subscribers from all months) ("always active")
#     4. (3) INTERSECT (intersection of subscribers from all months where location is not null) ("always locatable")
#     5. (4) INTERSECT (subscribers who spent more than 2 consecutive months at the same location) ("stable")
#     6. SELECT subscriber, coalesce(value, 'sometimes_unlocatable') AS value
#        FROM (3) always_active
#        LEFT JOIN (
#            SELECT subscriber, coalesce(value, 'highly_mobile') AS value
#            FROM (4) always_locatable
#            LEFT JOIN (
#                SELECT subscriber, 'stable' AS value
#                FROM (5) stable
#            ) labelled_stable
#            USING (subscriber)
#        ) labelled_always_locatable
#        USING (subscriber)
#        (etc.)
