#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



set -e

# if [ "$CI" != "true" ]; then
#     echo "Setting up docker containers"
#     echo "Bringing down any existing ones."
#     docker-compose -f docs-build-containers.yml down
#     echo "Bringing up new ones."
#     docker-compose -f docs-build-containers.yml up -d
#     echo "Waiting for flowdb to be ready"
#     docker exec flowkit_docs_flowdb bash -c 'i=0; until [ $i -ge 24 ] || (pg_isready -h 127.0.0.1 -p 5432); do let i=i+1; echo Waiting 10s; sleep 10; done'
#     echo "Starting build."
# fi

BRANCH=${CIRCLE_BRANCH:="master"} mkdocs build

# if [ "$CI" != "true" ]; then
#     echo "Bringing down containers."
#     docker-compose -f docs-build-containers.yml down
# fi

echo "Docs built. HTML in flowkit-docs"
