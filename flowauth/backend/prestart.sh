#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -e
export FLASK_APP=flowauth
if [ -n "$DEMO_MODE" ]
then
    echo "Creating demodata"
    if [ -z "$FLOWAUTH_FERNET_KEY" ]
    then
        echo "No FLOWAUTH_FERNET_KEY env var set, checking for a secret."
        if [ ! -e /run/secrets/FLOWAUTH_FERNET_KEY ]
        then
            echo "NO FLOWAUTH_FERNET_KEY PROVIDED. USING DEMO KEY."
            mkdir -p /run/secrets | true
            echo "XU-J5xNOtkaUKAoqWT7_VoT3zk2OTuoqKPBN3l0pOFg=" > /run/secrets/FLOWAUTH_FERNET_KEY
        fi
    fi
    flask demodata
else
    if [ -z "$FLOWAUTH_FERNET_KEY" ]
    then
        echo "No FLOWAUTH_FERNET_KEY env var set, checking for a secret."
        if [ ! -e /run/secrets/FLOWAUTH_FERNET_KEY ]
        then
            echo "NO FLOWAUTH_FERNET_KEY PROVIDED!"
            exit 1
        fi
    fi
    if [ -n "$INIT_DB" ]
    then
        echo "Recreating db"
        flask init-db --force
    else
        echo "Creating db"
        flask init-db
    fi
    if [ -e /run/secrets/ADMIN_USER ];
    then
        echo "Got admin username from secrets."
        ADMIN_USER=$(< /run/secrets/ADMIN_USER)
    fi
    if [ -e /run/secrets/ADMIN_PASSWORD ];
    then
        echo "Got admin password from secrets."
        ADMIN_PASSWORD=$(< /run/secrets/ADMIN_PASSWORD)
    fi
    if [ -n "$ADMIN_USER" ]
    then
        echo "Adding admin user."
        if [ -n "$ADMIN_PASSWORD" ]
        then
            flask add-admin
        else
            echo "No password for admin user. Do you need to set \$ADMIN_PASSWORD or supply a secret?"
            exit 1
        fi
    fi
fi



