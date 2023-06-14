# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

UpdateLocationIDsTableOperator = fixed_sql_operator(
    class_name="UpdateLocationIDsTableOperator",
    sql="""
        INSERT INTO events.location_ids (location_id, cdr_type, first_active_date, last_active_date)
        SELECT location_id,
               '{{ params.cdr_type }}' AS cdr_type,
               '{{ ds }}'::DATE AS first_active_date,
               '{{ ds }}'::DATE AS last_active_date
        FROM {{ final_table }}
        WHERE location_id IS NOT NULL
        GROUP BY location_id
        ON CONFLICT (location_id, cdr_type)
        DO UPDATE SET first_active_date = least(EXCLUDED.first_active_date, events.location_ids.first_active_date),
                      last_active_date = greatest(EXCLUDED.last_active_date, events.location_ids.last_active_date);
        """,
)
