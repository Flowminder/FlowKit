# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

CreateStagingViewOperator = wrapped_sql_operator(
    class_name="CreateStagingViewOperator",
    sql="CREATE OR REPLACE VIEW {{{{ staging_table }}}} AS {sql}",
)
