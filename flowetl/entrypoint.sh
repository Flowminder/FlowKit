#!/usr/bin/env bash

set -euo pipefail

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Used to fake a user and group when passed to docker and does not already exist
# https://cwrap.org/nss_wrapper.html

# allow the container to be started with `--user`
if [ "$1" = 'webserver' ] && [ "$(id -u)" = '0' ]; then
	  chown -R airflow "$AIRFLOW_HOME"
	  chmod 700 "$AIRFLOW_HOME"
fi

if [ "$1" = 'webserver' ]; then
    # allow to fail if not uid 0
	  chown -R airflow "$AIRFLOW_HOME" 2>/dev/null || :
	  chmod 700 "$AIRFLOW_HOME" 2>/dev/null || :
fi

if ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
    export LD_PRELOAD='/usr/lib/libnss_wrapper.so'
    export NSS_WRAPPER_PASSWD="$(mktemp)"
    export NSS_WRAPPER_GROUP="$(mktemp)"
		echo "airflow:x:$(id -u):$(id -g):Airflow:$HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
		echo "airflow:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
    chown $(id -u) /etc/authbind/byport/80
fi


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

# This is the original init script https://github.com/puckel/docker-airflow/blob/c6547079fef5b06ace9165e6b516c95a85cd0f16/script/entrypoint.sh
# modified to create an admin user from env vars.

TRY_LOOP="20"

: "${REDIS_HOST:="redis"}"
: "${REDIS_PORT:="6379"}"
: "${REDIS_PASSWORD:=""}"

# Make sure we have a db connection specified

: "${AIRFLOW__CORE__SQL_ALCHEMY_CONN:?AIRFLOW__CORE__SQL_ALCHEMY_CONN env var or secret must be set.}"

# Defaults and back-compat
: "${AIRFLOW_HOME:="/opt/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

export \
  AIRFLOW_HOME \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \


# Load DAGs exemples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user -r /requirements.txt
fi

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}



if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
  AIRFLOW__CELERY__BROKER_URL="redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1"
  wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"
fi

case "$1" in
  webserver)
    airflow initdb
    sleep 10
    airflow create_user -r Admin -u "${FLOWETL_AIRFLOW_ADMIN_USERNAME:?Need to set FLOWETL_AIRFLOW_ADMIN_USERNAME non-empty}" -e ${FLOWETL_AIRFLOW_ADMIN_EMAIL:-admin@example.com} -f ${FLOWETL_AIRFLOW_ADMIN_FIRSTNAME:-admin} -l ${FLOWETL_AIRFLOW_ADMIN_LASTNAME:-user} -p "${FLOWETL_AIRFLOW_ADMIN_PASSWORD:?Need to set FLOWETL_AIRFLOW_ADMIN_PASSWORD non-empty}"
    airflow pool -s "postgres_etl" ${FLOWETL_AIRFLOW_PG_POOL_SLOT_COUNT:-4} "Allows an upper bound on number of concurrent high load postgres dags."
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi
    exec authbind --deep airflow webserver -p ${FLOWETL_PORT}
    ;;
  worker|scheduler)
    # To give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac

