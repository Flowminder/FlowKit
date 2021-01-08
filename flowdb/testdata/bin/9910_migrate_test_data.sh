#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



set -e
export PGUSER="$POSTGRES_USER"

python3 /docker-entrypoint-initdb.d/migrate_synth_data.py