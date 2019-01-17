#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


#
#  Copy the file 'postgresql.configurator.conf' (which contains custom
#  configuration settings for the database) to its intended
#  location var/lib/postgresql/data/postgresql.configurator.conf, and add
#  a line to the main postgres configuration file at
#  /var/lib/postgresql/data/postgresql.conf to include it.
#
#  We have to do things in this roundabout way because the
#  postgres Docker container mounts an external volume to
#  the directory /var/lib/postgresql/data/. Therefore we
#  can't copy anything to this directory in the Dockerfile
#  directly because the change won't take effect.
#

#
#  Generates config file using the configurate.py script
#  and adds it to the PostgreSQL general configuration.
#
set -e

python3 /docker-entrypoint-initdb.d/configurate.py
echo "include 'postgresql.configurator.conf'" >> /var/lib/postgresql/data/postgresql.conf
