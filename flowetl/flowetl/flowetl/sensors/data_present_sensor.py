# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

DataPresentSensor = fixed_sql_operator(
    class_name="DataPresentSensor",
    sql="SELECT * FROM {{ staging_table }} LIMIT 1;",
    is_sensor=True,
)
