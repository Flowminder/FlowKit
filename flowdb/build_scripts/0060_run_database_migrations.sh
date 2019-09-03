#!/bin/bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -euo pipefail

#
#  FLOWDB DATABASE MIGRATIONS
#  --------------------------
#
#  Run database migrations using alembic.
#

# Restart postgres and make it listen on localhost
# so that Alembic can connect to it.
pg_ctl -w \
    -D "$PGDATA" \
    -o "-c listen_addresses='127.0.0.1'" \
	restart

echo "---------------------------------"
echo " * Applying database migrations. "
echo "---------------------------------"

cd /docker-entrypoint-initdb.d/
alembic upgrade head
