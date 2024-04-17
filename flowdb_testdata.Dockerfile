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
#   Install pyenv to avoid being pinned to debian python
#

RUN apt update && apt install git -y --no-install-recommends && \
    curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge -y --auto-remove

#
# Install python dependencies
#
COPY --chown=postgres flowdb/testdata/test_data/Pipfile* /docker-entrypoint-initdb.d/
USER postgres
RUN cd /docker-entrypoint-initdb.d/ && pipenv install --clear --deploy
USER root
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
COPY --chown=postgres flowdb/testdata/test_data/data/*.csv /docker-entrypoint-initdb.d/data/csv/
COPY --chown=postgres flowetl/flowetl/flowetl/qa_checks /docker-entrypoint-initdb.d/qa_checks
# Need to make postgres owner of any subdirectories
RUN chown -R postgres /docker-entrypoint-initdb.d
# Explicitly set number of days of dfs data to match test data
ENV N_DAYS=7
