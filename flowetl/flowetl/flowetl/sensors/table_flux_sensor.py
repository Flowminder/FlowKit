# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

TableFluxSensor = fixed_sql_operator_with_params(
    class_name="TableFluxSensor",
    sql="""
                CREATE TEMPORARY TABLE {{ staging_table_name }}_count AS
                SELECT count(*) FROM {{ staging_table }};
                SELECT pg_sleep({{ params.flux_check_interval }});
                SELECT 1 WHERE (SELECT * FROM {{ staging_table_name }}_count) = (SELECT count(*) FROM {{ staging_table }});
                """,
    params=["flux_check_interval"],
    is_sensor=True,
)
