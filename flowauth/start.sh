#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -euo pipefail

npm_command="${1:-start}"
if [ "$#" -gt "1" ]; then
shift
npm_command_args="$@"
fi

KillJobs() {
    for job in $(jobs -p); do
    		echo "Killing $job"
            kill -s SIGTERM $job > /dev/null 2>&1 || (sleep 10 && kill -9 $job > /dev/null 2>&1 &)

    done
}

TrapQuit() {
	echo "Shutting down FlowAuth"
    KillJobs
}

trap TrapQuit EXIT

# Spin up backend if running tests, or starting for dev mode
if [ "$npm_command" = "start" ] || [ "$npm_command" = "test" ];then
    echo "Installing flowauth dependencies"
    pipenv install -d
    echo "Starting FlowAuth"
    DEMO_DATA=true pipenv run flask run --port 3001 &
fi
pushd frontend
if [ "$npm_command" = "test" ];then
    npm run-script start &
fi
echo "Starting npm"
npm run-script $npm_command -- $npm_command_args
