#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -e


if [ $# -gt 0 ] && [ "$1" = "stop" ]
then
    export DOCKER_FLOWDB_HOST=flowdb_testdata
    echo "Stopping containers"
    curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/docker-compose.yml | docker-compose -f - down
else
    set -a
    source /dev/stdin <<< "$(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/development_environment)"
    export DOCKER_FLOWDB_HOST=flowdb_testdata
    if [ "$CI" != "true" ]; then
        export CONTAINER_TAG=$CIRCLE_SHA1
    fi
    echo "Starting containers"
    curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/${BRANCH:-master}/docker-compose.yml | docker-compose -f - up -d flowdb_testdata flowapi flowmachine flowauth query_locker
    echo "Waiting for containers to be ready.."
    docker exec ${DOCKER_FLOWDB_HOST} bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done' || (>&2 echo "FlowDB failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowDB,bug including:" && docker logs flowdb && exit 1)
    curl -s http://localhost:$FLOWAPI_PORT/api/0/spec/openapi-redoc.json > /dev/null || (>&2 echo "FlowAPI failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAPI,bug including:" && docker logs flowapi && exit 1)
    curl -s http://localhost:$FLOWAUTH_PORT > /dev/null || (>&2 echo "FlowAuth failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowAuth,bug including:" && docker logs flowauth && exit 1)
    nc -z localhost $FLOWMACHINE_PORT > /dev/null || (>&2 echo "FlowMachine failed to start :( Please open an issue at https://github.com/Flowminder/FlowKit/issues/new?template=bug_report.md&labels=FlowMachine,bug including:" && docker logs flowmachine && exit 1)
    echo "Containers ready!"
    echo "Access FlowDB using 'PGHOST=$FLOWDB_HOST PGPORT=$FLOWDB_PORT PGDATABASE=flowdb PGUSER=$FLOWMACHINE_FLOWDB_USER PGPASSWORD=$FLOWMACHINE_FLOWDB_PASSWORD psql'"
    echo "Access FlowAPI using FlowClient at http://localhost:$FLOWAPI_PORT"
    echo "View the FlowAPI spec at http://localhost:$FLOWAPI_PORT/api/0/spec/redoc"
    echo "Generate FlowAPI access tokens using FlowAuth with user TEST_USER and password DUMMY_PASSWORD at http://localhost:$FLOWAUTH_PORT"
fi