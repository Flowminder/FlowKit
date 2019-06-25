# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWDB
#  -----
#
#
#  This image is based on the official
#  PostgreSQL image, which is based
#  on the official Debian Stretch (9) image.
#
ARG POSTGRES_RELEASE=11
ARG POSTGRES_DIGEST=fb0ff33f663bcb5bc4962cac44ccffb57f4523c1fc2a17b0504db1c07c1d2666

FROM postgres:$POSTGRES_RELEASE@sha256:$POSTGRES_DIGEST

ARG POSTGRES_RELEASE
ENV POSTGRES_RELEASE=$POSTGRES_RELEASE
ARG POSTGIS_MAJOR=2.5
ENV POSTGIS_MAJOR=$POSTGIS_MAJOR
ARG POSTGIS_VERSION=2.5.2
ENV POSTGIS_VERSION=$POSTGIS_VERSION
ENV POSTGRES_DB=flowdb
ARG POSTGRES_USER=flowdb
ENV POSTGRES_USER=$POSTGRES_USER
ENV LC_ALL=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8

RUN apt-get update \
        && apt-get install -y --no-install-recommends postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR postgis postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts gdal-bin postgresql-$PG_MAJOR-pgrouting \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get purge -y --auto-remove

#
#  Setting up locale settings. This will
#  eventually fix encoding issues.
#
RUN apt-get update && apt-get install -y --no-install-recommends locales locales-all \
        && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
        && locale-gen && rm -rf /var/lib/apt/lists/*

#
#  INSTALLING DEPENDENCIES
#  -----------------------
#
#  In this section we install the dependencies
#  that the database will use when analysing
#  data.

#
# Install some useful extras & python dependencies
#
RUN apt-get update \
        && apt-get install -y --no-install-recommends \
        unzip curl make postgresql-plpython-$PG_MAJOR \
        postgresql-$PG_MAJOR-cron \
        libaio1  \
        parallel nano vim python3-pip wget python3-setuptools\
        && apt purge -y --auto-remove \
        && rm -rf /var/lib/apt/lists/*

# add requirements for pg_admin debugging
ENV USE_PGXS=1
RUN apt-get update \
        && apt-get install -y --no-install-recommends libssl-dev \
        libkrb5-dev \ 
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        git \
        && git clone https://git.postgresql.org/git/pldebugger.git \
        && mv pldebugger /usr/local/src \
        && make -C /usr/local/src/pldebugger \
        && make -C /usr/local/src/pldebugger install \
        && apt-get remove -y libssl-dev \
        libkrb5-dev \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        git \
        && apt purge -y --auto-remove \
        && rm -rf /var/lib/apt/lists/*


#
#  CONFIGURATION
#  -------------
#
#  In this section packages installed in previous
#  steps are properly configured. That happens by
#  either modifying configuration files (*.config)
#  or by loading *.sh scripts that will gradually
#  do that.
#
RUN mkdir -p /docker-entrypoint-initdb.d

#
#  Let's now install the `flowdb-cli` program
#  and run its automatic configuration command.
#
#  pipenv uses the first pip and python found in the
#  PATH so we need to force the right python version
#  together with the appropriate headers, otherwise
#  psutil fails to install.
#
COPY ./Pipfile* /tmp/
RUN apt-get update \
        && apt-get install -y --no-install-recommends python3-dev gcc m4 libxml2-dev libaio-dev \
        && pip3 install pgxnclient \
        && pip3 install pipenv \
        && PIPENV_PIPFILE=/tmp/Pipfile pipenv install --system --deploy --three \
        && apt-get remove -y python3-dev gcc m4 libxml2-dev libaio-dev \
        && apt purge -y --auto-remove \
        && rm -rf /var/lib/apt/lists/*

# Version Information
# Set the version & release date
ARG FLOWDB_VERSION=v1.7.2
ENV FLOWDB_VERSION=$FLOWDB_VERSION
ARG FLOWDB_RELEASE_DATE='3000-12-12'
ENV FLOWDB_RELEASE_DATE=$FLOWDB_RELEASE_DATE

# Default users

ENV FLOWMACHINE_FLOWDB_USER=flowmachine
ENV FLOWAPI_FLOWDB_USER=flowapi

# Default location table
ENV LOCATION_TABLE=infrastructure.cells


#
#  Copy file spinup build scripts to be execed.
#
COPY --chown=postgres ./bin/build/* /docker-entrypoint-initdb.d/

#
#  Add local data to PostgreSQL data ingestion
#  directory. Files in that directory will be
#  ingested by PostgreSQL on build-time.
#
ADD --chown=postgres ./sql/* /docker-entrypoint-initdb.d/
ADD --chown=postgres ./data/* /docker-entrypoint-initdb.d/data/csv/
# Need to make postgres owner
RUN chown -R postgres /docker-entrypoint-initdb.d

EXPOSE 5432
