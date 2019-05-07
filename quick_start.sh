#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -e
set -a



# Read a single char from /dev/tty, prompting with "$*"
# Note: pressing enter will return a null string. Perhaps a version terminated with X and then remove it in caller?
# See https://unix.stackexchange.com/a/367880/143394 for dealing with multi-byte, etc.
function get_keypress {
  local REPLY IFS=
  >/dev/tty printf '%s' "$*"
  [[ $ZSH_VERSION ]] && read -rk1  # Use -u0 to read from STDIN
  # See https://unix.stackexchange.com/q/383197/143394 regarding '\n' -> ''
  [[ $BASH_VERSION ]] && </dev/tty read -rn1
  printf '%s' "$REPLY"
}

# Get a y/n from the user, return yes=0, no=1 enter=$2
# Prompt using $1.
# If set, return $2 on pressing enter, useful for cancel or defualting
function get_yes_keypress {
  local prompt="${1:-Are you sure [y/n]? }"
  local enter_return=$2
  local REPLY
  # [[ ! $prompt ]] && prompt="[y/n]? "
  while REPLY=$(get_keypress "$prompt"); do
    [[ $REPLY ]] && printf '\n' # $REPLY blank if user presses enter
    case "$REPLY" in
      Y|y)  return 0;;
      N|n)  return 1;;
      '')   [[ $enter_return ]] && return "$enter_return"
    esac
  done
}

# Credit: http://unix.stackexchange.com/a/14444/143394
# Prompt to confirm, defaulting to NO on <enter>
# Usage: confirm "Dangerous. Are you sure?" && rm *
function confirm {
  local prompt="${*:-Are you sure} [y/N]? "
  get_yes_keypress "$prompt" 1
}

export CONTAINER_TAG=$CONTAINER_TAG
if [ "$CI" = "true" ]; then
    export CONTAINER_TAG=$CIRCLE_SHA1
    export BRANCH=$CIRCLE_SHA1
fi

if [ $# -gt 0 ] && [ "$1" = "larger_data" ] || [ "$2" = "larger_data" ]
then
    export DOCKER_FLOWDB_HOST=flowdb_synthetic_data
else
    export DOCKER_FLOWDB_HOST=flowdb_testdata
fi

if [ $# -gt 0 ] && [ "$1" = "examples" ] || [ "$2" = "examples" ]
then
    export WORKED_EXAMPLES=worked_examples
    if [ $# -gt 0 ] && [ "$1" = "smaller_data" ] || [ "$2" = "smaller_data" ]
    then
        export DOCKER_FLOWDB_HOST=flowdb_testdata
    else
        export DOCKER_FLOWDB_HOST=flowdb_testdata
    fi
else
    export WORKED_EXAMPLES=
fi

DOCKER_ENGINE_VERSION=`docker version --format '{{.Server.Version}}'`
DOCKER_COMPOSE_VERSION=`docker-compose version --short`
if [[ "$DOCKER_ENGINE_VERSION" < "17.12.0" ]]
then
    echo "Docker version not supported. Please upgrade docker to at least v17.12.0"
    exit 1
fi

if [[ "$DOCKER_COMPOSE_VERSION" < "1.21.0" ]]
then
    echo "docker-compose version not supported. Please upgrade docker to at least v1.21.0 (e.g. by running 'pip install --upgrade docker-compose'"
    echo "or installing a newer version of Docker desktop."
    exit 1
fi


if [ $# -gt 0 ] && [ "$1" = "stop" ]
then
    export DOCKER_FLOWDB_HOST=flowdb_testdata
    echo "Stopping containers"
    source /dev/stdin <<< "$(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/development_environment)"
    curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/docker-compose.yml | docker-compose -f - down
else
    source /dev/stdin <<< "$(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/development_environment)"
    echo "Starting containers"
    RUNNING=`curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/docker-compose.yml | docker-compose -f - ps -q $DOCKER_FLOWDB_HOST flowapi flowmachine flowauth flowmachine_query_locker $WORKED_EXAMPLES`
    if [[ "$RUNNING" != "" ]]; then
        confirm "Existing containers are running and will be replaced. Are you sure?" || exit 1
    fi
    curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/docker-compose.yml | docker-compose -f - up -d $DOCKER_FLOWDB_HOST flowapi flowmachine flowauth flowmachine_query_locker $WORKED_EXAMPLES
    echo "Waiting for containers to be ready.."
    docker exec ${DOCKER_FLOWDB_HOST} bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done' || (>&2 echo "FlowDB failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowDB,bug including the output of running 'docker logs flowdb'" && exit 1)
    echo "FlowDB ready."
    i=0; until [ $i -ge 24 ] || (netstat -an | grep -q $FLOWMACHINE_PORT) ; do let i=i+1; echo Waiting 10s; sleep 10; done || (>&2 echo "FlowMachine failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowMachine,bug including the output of running 'docker logs flowmachine'" && exit 1)
    echo "FlowMachine ready"
    i=0; until [ $i -ge 24 ] || (curl -s http://localhost:$FLOWAPI_PORT/api/0/spec/openapi.json > /dev/null) ; do let i=i+1; echo Waiting 10s; sleep 10; done || (>&2 echo "FlowAPI failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAPI,bug including the output of running 'docker logs flowapi'" && exit 1)
    echo "FlowAPI ready."
    i=0; until [ $i -ge 24 ] || (curl -s http://localhost:$FLOWAUTH_PORT > /dev/null) ; do let i=i+1; echo Waiting 10s; sleep 10; done || (>&2 echo "FlowAuth failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAuth,bug including the output of running 'docker logs flowauth'" && exit 1)
    echo "FlowAuth ready."
    if [[ "$WORKED_EXAMPLES" = "worked_examples" ]]
    then
        i=0; until [ $i -ge 24 ] || (curl -s http://localhost:$WORKED_EXAMPLES_PORT > /dev/null) ; do let i=i+1; echo Waiting 10s; sleep 10; done || (>&2 echo "Worked examples failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=docs,bug including the output of running 'docker logs worked_examples'" && exit 1)
    fi
    echo "Worked examples ready."
    echo "All containers ready!"
    echo "Access FlowDB using 'PGHOST=$FLOWDB_HOST PGPORT=$FLOWDB_PORT PGDATABASE=flowdb PGUSER=$FLOWMACHINE_FLOWDB_USER PGPASSWORD=$FLOWMACHINE_FLOWDB_PASSWORD psql'"
    echo "Access FlowAPI using FlowClient at http://localhost:$FLOWAPI_PORT"
    echo "View the FlowAPI spec at http://localhost:$FLOWAPI_PORT/api/0/spec/redoc"
    echo "Generate FlowAPI access tokens using FlowAuth with user TEST_USER and password DUMMY_PASSWORD at http://localhost:$FLOWAUTH_PORT"
    if [[ "$WORKED_EXAMPLES" = "worked_examples" ]]
    then
        echo "Try out the interactive examples at http://localhost:$WORKED_EXAMPLES_PORT"
    fi
fi