#!/bin/bash
set -euo pipefail
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


#
#  FLOWDB VERSION
#  -------------
#
#  Adds version number into a flowdb
#  version table. This data is eventually
#  used by the flowdb_version() function.
#

export PGUSER="$POSTGRES_USER"


psql --dbname="$POSTGRES_DB" -c "
    BEGIN;
        CREATE TABLE IF NOT EXISTS flowdb_version (
            version TEXT PRIMARY KEY,
            release_date DATE,
            updated BOOL
        );
        INSERT INTO flowdb_version (version, release_date)
            VALUES ('$FLOWDB_VERSION', '$FLOWDB_RELEASE_DATE')
              ON CONFLICT (version)
              DO UPDATE SET updated = True;
    END;
"
