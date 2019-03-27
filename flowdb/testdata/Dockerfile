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

RUN mkdir -p \
    /docker-entrypoint-initdb.d/sql/testdata/ \
    /docker-entrypoint-initdb.d/py/testdata/
COPY --chown=postgres ./bin/z_ingest_test_data.sh /docker-entrypoint-initdb.d/
COPY --chown=postgres bin/zz_run_synthetic_dfs_data_generation_script.sh /docker-entrypoint-initdb.d/
COPY --chown=postgres bin/zz_generate_synthetic_dfs_data.py /docker-entrypoint-initdb.d/py/testdata/
ADD --chown=postgres ./test_data/sql/* /docker-entrypoint-initdb.d/sql/testdata/
ADD --chown=postgres ./test_data/data/ /docker-entrypoint-initdb.d/data/
COPY  --chown=postgres ./test_data/data/*.csv /docker-entrypoint-initdb.d/data/csv/
# Need to make postgres owner of any subdirectories
RUN chown -R postgres /docker-entrypoint-initdb.d
