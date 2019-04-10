#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -e


TrapQuit() {
    if [ "$CI" != "true" ]; then
	    echo "Bringing down containers."
	    docker-compose down
	fi
}

trap TrapQuit EXIT

if [ "$CI" != "true" ]; then
    echo "Setting up docker containers"
    echo "Bringing up new ones."
    (pushd .. && make up flowdb_testdata query_locker && popd)
    echo "Waiting for flowdb to be ready"
    docker exec flowdb_testdata bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
fi
echo "Installing."
pipenv install --deploy
echo "Running tests."
pipenv run pytest $@
