#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -e
set -a

#
# Helper functions to ask user for confirmation
#

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

#
# Set git revision / container tag to be used. Note that the
# environment variable CONTAINER_TAG is also passed on to
# the docker-compose file which is downloaded and run below.
# Typically the values of CONTAINER_TAG and GIT_REVISION are
# identical (referring to the SHA-1 checksum of the git revision
# to be deployed), but if we want to deploy the latest master
# we need to use GIT_REVISION=master and CONTAINER_TAG=latest.
#
if [ "$CI" = "true" ]; then
    export GIT_REVISION=$CIRCLE_SHA1
else
    export GIT_REVISION=${GIT_REVISION:-master}
fi
if [ "$GIT_REVISION" = "master" ]; then
    export CONTAINER_TAG="latest"
else
    export CONTAINER_TAG=$GIT_REVISION
fi

if [ $# -gt 0 ] && [ "$1" = "larger_data" ] || [ "$2" = "larger_data" ]
then
    export EXTRA_COMPOSE=docker-compose-syntheticdata.yml
else
    export EXTRA_COMPOSE=docker-compose-testdata.yml
fi

if [ $# -gt 0 ] && [ "$1" = "examples" ] || [ "$2" = "examples" ]
then
    export WORKED_EXAMPLES=worked_examples
    if [ $# -gt 0 ] && [ "$1" = "smaller_data" ] || [ "$2" = "smaller_data" ]
    then
        export EXTRA_COMPOSE=docker-compose-testdata.yml
    else
        export EXTRA_COMPOSE=docker-compose-syntheticdata.yml
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

DOCKER_WORKDIR=`mktemp -d`
if [[ ! "$DOCKER_WORKDIR" || ! -d "$DOCKER_WORKDIR" ]]
then
    echo "Could not create temporary docker working directory."
    exit 1
fi

function dockertemp_cleanup {
    rm -rf "$DOCKER_WORKDIR"
}
trap dockertemp_cleanup EXIT

curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${GIT_REVISION}/docker-compose.yml -o "$DOCKER_WORKDIR/docker-compose.yml"
curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${GIT_REVISION}/docker-compose-testdata.yml -o "$DOCKER_WORKDIR/docker-compose-testdata.yml"
curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${GIT_REVISION}/docker-compose-syntheticdata.yml -o "$DOCKER_WORKDIR/docker-compose-syntheticdata.yml"

DOCKER_COMPOSE="docker-compose -p flowkit_qs -f $DOCKER_WORKDIR/docker-compose.yml -f $DOCKER_WORKDIR/$EXTRA_COMPOSE"

if [ $# -gt 0 ] && [ "$1" = "stop" ]
then
    echo "Stopping containers"
    source /dev/stdin <<< "$(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${GIT_REVISION}/development_environment)"
    $DOCKER_COMPOSE down -v
else
    source /dev/stdin <<< "$(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${GIT_REVISION}/development_environment)"
    echo "Starting containers (this may take a few minutes)"
    RUNNING=`$DOCKER_COMPOSE ps -q flowdb flowapi flowmachine flowauth flowmachine_query_locker $WORKED_EXAMPLES`
    if [[ "$RUNNING" != "" ]]; then
        confirm "Existing containers are running and will be replaced. Are you sure?" || exit 1
    fi
    DOCKER_SERVICES="flowdb flowapi flowmachine flowauth flowmachine_query_locker $WORKED_EXAMPLES"
    $DOCKER_COMPOSE pull $DOCKER_SERVICES
    $DOCKER_COMPOSE up -d --renew-anon-volumes $DOCKER_SERVICES
    echo "Waiting for containers to be ready.."
    docker exec flowdb bash -c 'i=0; until { [ $i -ge 90 ] && exit_status=1; } || { (pg_isready -h 127.0.0.1 -p 5432) && exit_status=0; }; do let i=i+1; echo Waiting 10s; sleep 10; done; exit $exit_status' || (>&2 echo "FlowDB failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowDB,bug including the output of running 'docker logs flowdb'" && exit 1)
    echo "FlowDB ready."
    (i=0; until { [ $i -ge 24 ] && exit_status=1; } || { ((ss -tuna || netstat -an) | grep -q $FLOWMACHINE_PORT) && exit_status=0; } ; do let i=i+1; echo Waiting 10s; sleep 10; done; exit $exit_status) || (>&2 echo "FlowMachine failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowMachine,bug including the output of running 'docker logs flowmachine'" && exit 1)
    echo "FlowMachine ready"
    (i=0; until { [ $i -ge 24 ] && exit_status=1; } || { (curl -s http://localhost:$FLOWAPI_PORT/api/0/spec/openapi.json > /dev/null) && exit_status=0; } ; do let i=i+1; echo Waiting 10s; sleep 10; done; exit $exit_status) || (>&2 echo "FlowAPI failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAPI,bug including the output of running 'docker logs flowapi'" && exit 1)
    echo "FlowAPI ready."
    (i=0; until { [ $i -ge 24 ] && exit_status=1; } || { (curl -s http://localhost:$FLOWAUTH_PORT > /dev/null) && exit_status=0; } ; do let i=i+1; echo Waiting 10s; sleep 10; done; exit $exit_status) || (>&2 echo "FlowAuth failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAuth,bug including the output of running 'docker logs flowauth'" && exit 1)
    echo "FlowAuth ready."
    if [[ "$WORKED_EXAMPLES" = "worked_examples" ]]
    then
        (i=0; until { [ $i -ge 24 ] && exit_status=1; } || { (curl -s http://localhost:$WORKED_EXAMPLES_PORT > /dev/null) && exit_status=0; } ; do let i=i+1; echo Waiting 10s; sleep 10; done; exit $exit_status) || (>&2 echo "Worked examples failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=docs,bug including the output of running 'docker logs worked_examples'" && exit 1)
        echo "Worked examples ready."
    fi
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
