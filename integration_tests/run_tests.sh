#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -e

export DOCKER_SERVICES="flowdb_testdata flowmachine_query_locker"

TrapQuit() {
    if [ "$CI" != "true" ] && [ "$KEEP_CONTAINERS_ALIVE" != "true" ]; then
	    echo "Bringing down containers."
	    (pushd .. &&  make down && popd)
	fi
}

trap TrapQuit EXIT

if [ "$CI" != "true" ]; then
    echo "Setting up docker containers"
    echo "Bringing up new ones."
    (pushd .. &&  make down && make up && popd)
    echo "Waiting for flowdb to be ready"
    docker exec flowdb bash -c 'i=0; until [ $i -ge 240 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
fi
echo "Installing."
bundle install
pipenv install --deploy
echo "Running tests."
pipenv run pytest $@
