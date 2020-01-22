# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

UpdateETLTableOperator = fixed_sql_operator(
    class_name="UpdateETLTableOperator",
    sql="""
        INSERT INTO etl.etl_records (cdr_type, cdr_date, state, timestamp) VALUES ('{{ params.cdr_type }}', '{{ ds }}'::DATE, 'ingested', NOW());
        INSERT INTO available_tables (table_name, has_locations, has_subscribers{% if params.cdr_type in ['calls', 'sms']  %}, has_counterparts {% endif %}) VALUES ('{{ params.cdr_type }}', true, true{% if params.cdr_type in ['calls', 'sms']  %}, true {% endif %})
            ON conflict (table_name)
            DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers{% if params.cdr_type in ['calls', 'sms']  %}, has_counterparts=EXCLUDED.has_counterparts {% endif %};
        """,
)
