#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



set -e
export PGUSER="$POSTGRES_USER"




#
#  Ingest supporting synthetic data.
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

#
#  Generate synthetic data.
# Two generators are available, and toggled between using the SYNTHETIC_DATA_GENERATOR env var
#

if [ -f /opt/synthetic_data/generate_synthetic_data.py ] && [  "$SYNTHETIC_DATA_GENERATOR" = "python" ]; then
  python3 /opt/synthetic_data/generate_synthetic_data.py \
      --n-subscribers ${N_SUBSCRIBERS} \
      --n-cells ${N_CELLS} \
      --n-calls ${N_CALLS} \
      --subscribers-seed ${SUBSCRIBERS_SEED} \
      --cells-seed ${CELLS_SEED} \
      --calls-seed ${CALLS_SEED} \
      --n-days ${N_DAYS} \
      --output-root-dir ${OUTPUT_ROOT_DIR}
elif [ -f /opt/synthetic_data/generate_synthetic_data_sql.py ] && [  "$SYNTHETIC_DATA_GENERATOR" = "sql" ]; then
python3 /opt/synthetic_data/generate_synthetic_data_sql.py \
    --n-subscribers ${N_SUBSCRIBERS} \
    --n-cells ${N_CELLS} \
    --n-calls ${N_CALLS} \
    --n-days ${N_DAYS} \
    --n-tacs ${N_TACS} \
    --n-mds ${N_MDS} \
    --n-sms ${N_SMS} \
    --n-sites ${N_SITES} \
    --out-of-area-probability ${P_OUT_OF_AREA:-0.05}\
    --relocation-probability ${P_RELOCATE:-0.05}\
    --interactions-multiplier ${INTERACTIONS_MULTIPLIER:-5}\
    --disaster-zone ${DISASTER_REGION_PCOD:-"524 4 12"} \
    --disaster-start-date ${DISASTER_START:-"2015-01-01"} \
    --disaster-end-date ${DISASTER_END:-"2015-01-01"}
elif [ -f /opt/synthetic_data/generate_synthetic_data_realistic.py ] && [  "$SYNTHETIC_DATA_GENERATOR" = "realistic" ]; then
  python3 /opt/synthetic_data/generate_synthetic_data_realistic.py \
      --n-subscribers ${N_SUBSCRIBERS} \
      --n-days ${N_DAYS} \
      --n-tacs ${N_TACS} \
      --n-mean_calls ${N_MEAN_CALLS} \
      --n-sd_calls ${N_SD_CALLS} \
      --n-mean_sms ${N_MEAN_SMS} \
      --n-sd_sms ${N_SD_SMS} \
      --n-mean_mds ${N_MEAN_MDS} \
      --n-sd_mds ${N_SD_MDS} \
      --n-sites ${N_SITES} \
      --n-startdate ${N_STARTDATE}
else
    echo "Must set SYNTHETIC_DATA_GENERATOR environment variable to 'sql', 'python' or 'realistic'."
    exit 1
fi
