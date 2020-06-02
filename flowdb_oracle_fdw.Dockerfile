# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWDB
#  -----
#
#  Extends the base FlowDB image to include the oracle_fdw.
#  You must provide a URL to download the relevant Oracle
#  binaries from.
#
ARG CODE_VERSION=latest
FROM flowminder/flowdb:${CODE_VERSION}

#
#  Installs `oracle_fdw` for connecting to
#  Oracle instances.
#
ENV ORACLE_HOME=/usr/local/oracle/instantclient
ENV LD_LIBRARY_PATH=/usr/local/oracle/instantclient
ARG INSTANT_CLIENT_VERSION=12.2.0.1.0
ENV INSTANT_CLIENT_VERSION=$INSTANT_CLIENT_VERSION
ARG ORACLE_FDW_VERSION=2_2_0
ENV ORACLE_FDW_VERSION=$ORACLE_FDW_VERSION
ARG ORACLE_BINARY_SOURCE=.

ADD $ORACLE_BINARY_SOURCE/instantclient-basic-linux.x64-$INSTANT_CLIENT_VERSION.zip /tmp
ADD $ORACLE_BINARY_SOURCE/instantclient-sdk-linux.x64-$INSTANT_CLIENT_VERSION.zip /tmp
ADD $ORACLE_BINARY_SOURCE/instantclient-sqlplus-linux.x64-$INSTANT_CLIENT_VERSION.zip /tmp

#
#  Download and install oracle binaries, and the oracle_fdw
#
RUN apt-get update \
            && apt-get install --yes --no-install-recommends build-essential \
            && apt-get clean --yes \
            && apt-get autoclean --yes \
            && apt-get autoremove --yes \
            && rm -rf /var/cache/debconf/*-old \
            && rm -rf /var/lib/apt/lists/* \
            && cd /tmp \
            && unzip /tmp/instantclient-basic-linux.x64-$INSTANT_CLIENT_VERSION.zip -d /usr/local/oracle \
            && unzip /tmp/instantclient-sqlplus-linux.x64-$INSTANT_CLIENT_VERSION.zip -d /usr/local/oracle \
            && unzip /tmp/instantclient-sdk-linux.x64-$INSTANT_CLIENT_VERSION.zip -d /usr/local/oracle \
            && rm instantclient-*-linux.x64-$INSTANT_CLIENT_VERSION.zip \
            && ln -s /usr/local/oracle/instantclient_* /usr/local/oracle/instantclient \
            && ln -s /usr/local/oracle/instantclient/libclntsh.so.* /usr/local/oracle/instantclient/libclntsh.so \
            && ln -s /usr/local/oracle/instantclient/sqlplus /usr/bin/sqlplus \
            && wget https://github.com/laurenz/oracle_fdw/archive/ORACLE_FDW_$ORACLE_FDW_VERSION.zip -P /usr/lib/postgresql/$PG_MAJOR/lib/ \
            && cd /usr/lib/postgresql/$PG_MAJOR/lib/ \
            && unzip ORACLE_FDW_$ORACLE_FDW_VERSION.zip \
            && cd oracle_fdw-ORACLE_FDW_$ORACLE_FDW_VERSION \
            && make \
            && make install \
            && cd .. \
            && rm ORACLE_FDW_$ORACLE_FDW_VERSION.zip \
            && rm -rf oracle_fdw-ORACLE_FDW_$ORACLE_FDW_VERSION/ \
            && apt-get remove -y build-essential \
            && apt purge -y --auto-remove \
            && rm -rf /var/lib/apt/lists/*
COPY flowdb/oracle_fdw/000_create_oracle_fdw_extension.sql /docker-entrypoint-initdb.d/