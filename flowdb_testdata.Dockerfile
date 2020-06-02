# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWDB
#  -----
#
# Extends the basic FlowDB image to include a small amount of test data.
#

ARG CODE_VERSION=latest
FROM flowminder/flowdb:${CODE_VERSION}


#
#   Install Python 3.7 (needed to run the data generation scripts)
#

RUN echo "deb http://deb.debian.org/debian stable main" > /etc/apt/sources.list \
        && apt-get -y update \
        && apt-get -y install python3.7 python3.7-distutils python3-psutil \
        && pip3 install --no-cache-dir pipenv \
        && apt-get clean --yes \
        && apt-get autoclean --yes \
        && apt-get autoremove --yes \
        && rm -rf /var/cache/debconf/*-old \
        && rm -rf /var/lib/apt/lists/*

#
# Install python dependencies
#
COPY --chown=postgres flowdb/testdata/test_data/Pipfile* /tmp/
RUN PIPENV_PIPFILE=/tmp/Pipfile pipenv install --clear --system --deploy --three \
    && rm /tmp/Pipfile*

#
#   Add test data to the ingestion directory. 
#

RUN mkdir -p \
    /docker-entrypoint-initdb.d/sql/testdata/ \
    /docker-entrypoint-initdb.d/py/testdata/
COPY --chown=postgres flowdb/testdata/bin/* /docker-entrypoint-initdb.d/
ADD --chown=postgres flowdb/testdata/test_data/sql/* /docker-entrypoint-initdb.d/sql/testdata/
ADD --chown=postgres flowdb/testdata/test_data/py/* /docker-entrypoint-initdb.d/py/testdata/
ADD --chown=postgres flowdb/testdata/test_data/data/ /docker-entrypoint-initdb.d/data/
COPY  --chown=postgres flowdb/testdata/test_data/data/*.csv /docker-entrypoint-initdb.d/data/csv/
# Need to make postgres owner of any subdirectories
RUN chown -R postgres /docker-entrypoint-initdb.d
