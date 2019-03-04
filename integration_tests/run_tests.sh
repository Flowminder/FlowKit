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
	export PIPENV_DOTENV_LOCATION=$(pwd)/.env
    echo "Setting up docker containers"
    echo "Bringing down any existing ones."
    docker-compose down
    echo "Bringing up new ones."
    docker-compose up -d
    echo "Waiting for flowdb to be ready"
    docker exec flowkit_integration_tests_flowdb bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
else
	export PIPENV_DONT_LOAD_ENV=1
fi
echo "Installing."
pipenv install --deploy
echo "Running tests."
pipenv run pytest $@
