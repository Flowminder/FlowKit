# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

CreateIndexesOperator = fixed_sql_operator_with_params(
    class_name="CreateIndexesOperator",
    sql="""
                {% for index_column in params.index_columns %}
                    DROP INDEX IF EXISTS {{ table_name }}_{{ index_column }}_idx;
                    CREATE INDEX {{ table_name }}_{{ index_column }}_idx ON {{ extract_table }} ({{ index_column }});
                {% endfor %}
                """,
    params=["index_columns"],
)
