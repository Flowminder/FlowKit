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
#   Add test data to the ingestion directory. 
#

RUN mkdir -p /docker-entrypoint-initdb.d/sql/testdata/
COPY ./bin/z_ingest_test_data.sh /docker-entrypoint-initdb.d/
ADD ./test_data/sql/* /docker-entrypoint-initdb.d/sql/testdata/
ADD ./test_data/data/ /docker-entrypoint-initdb.d/data/
COPY ./test_data/data/*.csv /docker-entrypoint-initdb.d/data/csv/
