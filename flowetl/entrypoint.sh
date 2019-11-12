#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Used to fake a user and group when passed to docker and does not already exist
# https://cwrap.org/nss_wrapper.html

# allow the container to be started with `--user`
if [ "$1" = 'webserver' ] && [ "$(id -u)" = '0' ]; then
	  chown -R postgres "$AIRFLOW_HOME"
	  chmod 700 "$AIRFLOW_HOME"
fi

if [ "$1" = 'webserver' ]; then
    # allow to fail if not uid 0
	  chown -R postgres "$AIRFLOW_HOME" 2>/dev/null || :
	  chmod 700 "$AIRFLOW_HOME" 2>/dev/null || :
fi

if ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
		echo "airflow:x:$(id -u):$(id -g):Airflow:$HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
		echo "airflow:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
fi

# Load all secrets

shopt -s nullglob
FILES=/run/secrets/*

# Export the secrets as environment variables for the main entrypoint
for f in $FILES;
do
  echo "Loading secret $f."
  export $(basename "$f")=$(cat "$f")
done

exec /defaultentrypoint.sh "$@"
