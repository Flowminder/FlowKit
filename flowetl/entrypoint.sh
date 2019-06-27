#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

if [ "$(id -u)" = '0' ]; then
    chown -R airflow: /mounts/

    exec gosu airflow "$BASH_SOURCE" "$@"
fi;

exec /defaultentrypoint.sh "$@"
