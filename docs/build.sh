#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -e

export DOCKER_SERVICES="flowdb_synthetic_data flowmachine_query_locker"

KillJobs() {
    for job in $(jobs -p); do
    		echo "Killing $job"
            kill -s SIGTERM $job > /dev/null 2>&1 || (sleep 10 && kill -9 $job > /dev/null 2>&1 &)

    done
}

TrapQuit() {
    if [ "$CI" != "true" ] && [ "$KEEP_CONTAINERS_ALIVE" != "true" ]; then
	    echo "Bringing down containers."
	    (pushd .. && make down && popd)
	fi

	echo "Shutting down FlowMachine and FlowAPI"
    KillJobs
}

trap TrapQuit EXIT

if [ "$CI" != "true" ]; then
    (pushd .. && make down && make up && popd)
    echo "Waiting for flowdb to be ready"
    pipenv install
    docker exec flowdb bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
    echo "Finished pre-caching queries"
    pipenv run flowmachine &
    echo "Started FlowMachine."
    pipenv run hypercorn --bind 0.0.0.0:9090 "flowapi.main:create_app()" &
    echo "Started FlowAPI."
    sleep 5
fi

echo "Retrieving API spec"
curl http://localhost:9090/api/0/spec/openapi-redoc.json -o source/_static/openapi-redoc.json
echo "Starting build."

# Note: the DOCS_BRANCH variable is used by `mkdocs.yml` to pick up the correct git repositories for building API docs
DOCS_BRANCH=${CIRCLE_BRANCH:="master"} pipenv run mkdocs "${@:-build}"
