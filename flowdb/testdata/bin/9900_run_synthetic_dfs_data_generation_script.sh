#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -e
export PGUSER="$POSTGRES_USER"

#
#  Generate synthetic DFS data.
#
#  Note that the only purpose of this script is to
#  call the Python script which does the actual data
#  data generation, but we need this shell script as
#  a wrapper because the PostgreSQL entrypoint script
#  does not pick up .py files on its own.
#

export DIR=/docker-entrypoint-initdb.d/py/testdata/

echo "Running Python script to generate synthetic DFS data."
python3 ${DIR}/zz_generate_synthetic_dfs_data.py
