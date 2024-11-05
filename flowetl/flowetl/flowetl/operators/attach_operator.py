# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

AttachOperator = fixed_sql_operator(
    class_name="AttachOperator",
    sql="""
        DROP TABLE IF EXISTS {{ final_table }};
        ALTER TABLE {{ extract_table }} RENAME TO {{ table_name }};
        ALTER TABLE {{ etl_schema }}.{{ table_name }} SET SCHEMA {{ final_schema }};
        ALTER TABLE {{ parent_table }} ATTACH PARTITION {{ final_table }} FOR VALUES FROM ('{{ ds }}') TO ('{{ tomorrow_ds }}');
        """,
)
