#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Used to fake a user and group when passed to docker and does not already exist
# https://cwrap.org/nss_wrapper.html
if ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
		echo "airflow:x:$(id -u):$(id -g):Airflow:$HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
		echo "airflow:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
fi

exec /defaultentrypoint.sh "$@"
