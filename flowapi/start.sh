#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



if [ -e /run/secrets/cert-flowkit.pem ];
then
    hypercorn --bind 0.0.0.0:9090 --certfile /run/secrets/cert-flowkit.pem --keyfile /run/secrets/cert-flowkit.pem "flowapi.main:create_app()"
else
    echo "WARNING: No certificate file provided. Communications with the API server will NOT BE SECURE."
    hypercorn --bind 0.0.0.0:9090 "flowapi.main:create_app()"
fi