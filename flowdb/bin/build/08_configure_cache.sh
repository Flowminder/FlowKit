#!/usr/bin/env bash
set -e
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


#
#  Cache configuration
#  -------------
#
#  Sends cache controlling environment variables to the cache
#  settings table.
#

# Get available disk space where postgresql dir is mounted
avail=$(df --output=avail -B 1 "/var/lib/postgresql/data" | tail -n 1)
# Default cache to a tenth of available space
CACHE_SIZE=${CACHE_SIZE:-$(expr $avail / 10)}
# Default half-life to 1000
CACHE_HALF_LIFE=${CACHE_HALF_LIFE:-1000}
echo "Setting cache size to $CACHE_SIZE bytes"
echo "Setting cache half-life to $CACHE_HALF_LIFE"
export PGUSER="$FLOWDB_ADMIN_USER"
psql --dbname="$POSTGRES_DB" -c "
        UPDATE cache.cache_config set value='$CACHE_HALF_LIFE' WHERE key='half_life';
        UPDATE cache.cache_config set value='$CACHE_SIZE' WHERE key='cache_size';
        "
