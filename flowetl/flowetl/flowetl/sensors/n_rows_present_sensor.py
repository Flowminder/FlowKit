# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

NRowsPresentSensor = fixed_sql_operator_with_params(
    class_name="NRowsPresentSensor",
    sql="SELECT EXISTS(SELECT * FROM {{ staging_table }} LIMIT 1 (OFFSET {{ params.minimum_rows }} - 1));",
    is_sensor=True,
    params=["minimum_rows"],
)
