#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -e


KillJobs() {
    for job in $(jobs -p); do
    		echo "Killing $job"
            kill -s SIGTERM $job > /dev/null 2>&1 || (sleep 10 && kill -9 $job > /dev/null 2>&1 &)

    done
}

TrapQuit() {
    if [ "$CI" != "true" ]; then
	    echo "Bringing down containers."
	    #docker-compose -f docs-build-containers.yml down
	fi

	echo "Shutting down FlowMachine and FlowAPI"
    KillJobs
}

trap TrapQuit EXIT

if [ "$CI" != "true" ]; then
	export PIPENV_DOTENV_LOCATION=$(pwd)/.env
    echo "Setting up docker containers"
    echo "Bringing down any existing ones."
    docker-compose -f docs-build-containers.yml down
    echo "Bringing up new ones."
    docker-compose -f docs-build-containers.yml up -d
    echo "Waiting for flowdb to be ready"
    docker exec flowkit_docs_flowdb bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
else
	export PIPENV_DONT_LOAD_ENV=1
fi

pipenv install
pipenv run flowmachine &
echo "Started FlowMachine."
pipenv run quart run --port 9090 &
echo "Retrieving API spec"
sleep 5
curl http://localhost:9090/api/0/spec/openapi-redoc.json -o source/_static/openapi-redoc.json
echo "Started FlowAPI."
echo "Starting build."

BRANCH=${CIRCLE_BRANCH:="master"} pipenv run mkdocs "${@:-build}"
