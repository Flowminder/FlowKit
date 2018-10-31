#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



set -e

#
#  Generate synthetic data
#

if [ -f /opt/synthetic_data/generate_synthetic_data.py ]; then
  python3 /opt/synthetic_data/generate_synthetic_data.py \
      --n-subscribers ${N_SUBSCRIBERS} \
      --n-cells ${N_CELLS} \
      --n-calls ${N_CALLS} \
      --subscribers-seed ${SUBSCRIBERS_SEED} \
      --cells-seed ${CELLS_SEED} \
      --calls-seed ${CALLS_SEED} \
      --n-days ${N_DAYS} \
      --output-root-dir ${OUTPUT_ROOT_DIR}
fi


#
#  Ingest synthetic data.
#

DIR="/docker-entrypoint-initdb.d/sql/syntheticdata"
count=0
if [ -d "$DIR" ]; then
  count=`ls -1 $DIR | grep \.sql | wc -l`
fi
if [ $count != 0 ]; then
  echo "Found $count SQL data files in directory."
  if [ "$(ls -A $DIR)" ]; then
      cd $DIR
      for f in *.sql
      do
        echo "Running"
        echo $f
        psql --dbname="$POSTGRES_DB" -f $f
        echo "------------- // Done // ---------------"
      done
  else
      echo "$DIR is empty."
  fi
fi
