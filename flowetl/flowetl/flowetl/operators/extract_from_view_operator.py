# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

ExtractFromViewOperator = wrapped_sql_operator(
    class_name="ExtractFromViewOperator",
    sql="""
        DROP TABLE IF EXISTS {{{{ extract_table }}}};
        CREATE TABLE {{{{ extract_table }}}} AS (
            {sql});
        
        DROP VIEW {{{{ staging_table }}}};
        """,
)
