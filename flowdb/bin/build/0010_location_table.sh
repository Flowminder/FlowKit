#!/bin/bash
set -euo pipefail
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


#
#  LOCATION TABLE
#  -------------
#
#  Adds a table identifying which table the location_id column
#  references. Used in get_location_table().
#

export PGUSER="$POSTGRES_USER"

psql --dbname="$POSTGRES_DB" -c "
    BEGIN;
        CREATE TABLE IF NOT EXISTS location_table (
            location_table TEXT PRIMARY KEY,
            changed BOOL
        );
        INSERT INTO location_table (location_table)
            VALUES ('$LOCATION_TABLE')
              ON CONFLICT (location_table)
              DO UPDATE SET changed = True;
    END;
"
