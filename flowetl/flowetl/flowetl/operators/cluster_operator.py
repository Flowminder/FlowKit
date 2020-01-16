# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

ClusterOperator = fixed_sql_operator_with_params(
    class_name="ClusterOperator",
    sql="""
                DROP INDEX IF EXISTS {{ table_name }}_{{ params.cluster_field }}_idx;
                CREATE INDEX {{ table_name }}_{{ params.cluster_field }}_idx ON {{ extract_table }} ({{ params.cluster_field }});
                CLUSTER {{ extract_table }} USING {{ table_name }}_{{ params.cluster_field }}_idx;
                """,
    params=["cluster_field"],
)
