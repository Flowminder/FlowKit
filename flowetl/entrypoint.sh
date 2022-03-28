#!/usr/bin/env bash

set -euo pipefail

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Load all secrets

shopt -s nullglob
FILES=/run/secrets/*

# Export the secrets as environment variables for the main entrypoint
for f in $FILES;
do
  echo "Loading secret $f."
  SECRET_NAME=$(basename "$f")
  if [[ $f == *_FILE ]]
  then
    echo "$f is a file secret."
    export ${SECRET_NAME%?????}="$f"
  else
    echo "Setting $SECRET_NAME"
    export $SECRET_NAME=$(cat "$f")
  fi
done

# Make sure we have a db connection specified

: "${AIRFLOW__CORE__SQL_ALCHEMY_CONN:?AIRFLOW__CORE__SQL_ALCHEMY_CONN env var or secret must be set.}"

## Defaults and back-compat
# These could mask missing
#: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
#: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"
#: "${AIRFLOW__WEBSERVER__SECRET_KEY:=${AIRFLOW__WEBSERVER__SECRET_KEY:=$(python -c "import os; print(os.urandom(16))")}}"

# Aliasing flowetl-specific secrets to their general use

: "REDIS_PASSWORD"=${FLOWETL_REDIS_PASSWORD:?FLOWETL_REDIS_PASSWORD not defined; check secrets}
: "POSTGRES_PASSWORD"=${FLOWETL_POSTGRES_PASSWORD:?FLOWETL_POSTGRES_PASSWORD not defined; check secrets}
: "SECRET_KEY"

export \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__WEBSERVER__SECRET_KEY \
  REDIS_PASSWORD \
  POSTGRES_PASSWORD \
  FLOWETL_CELERY_PASSWORD

echo "Running airflow entrypoint."
exec /entrypoint "${@}"