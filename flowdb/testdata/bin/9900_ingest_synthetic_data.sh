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

COUNTRY=${COUNTRY:-"NPL"}
wget --retry-connrefused -t=5 "https://geodata.ucdavis.edu/gadm/gadm3.6/shp/gadm36_${COUNTRY}_shp.zip" -O /docker-entrypoint-initdb.d/data/geo.zip
unzip /docker-entrypoint-initdb.d/data/geo.zip -d /docker-entrypoint-initdb.d/data/geo
echo $(ls /docker-entrypoint-initdb.d/data/)
pipenv run python /opt/synthetic_data/generate_synthetic_data_sql.py \
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
    --disaster-zone \"${DISASTER_REGION_PCOD:-"NPL.1.1_1"}\" \
    --disaster-start-date ${DISASTER_START:-"2015-01-01"} \
    --disaster-end-date ${DISASTER_END:-"2015-01-01"} \
    --country ${COUNTRY} || (cat cat /var/lib/postgresql/data/pg_log/postgres-* && exit 1)

if [ "${SKIP_TEST_QA_CHECK}" != "true" ]; then
   cd /docker-entrypoint-initdb.d
   echo "Running QA checks on test data"
   pipenv run python run_qa_checks.py qa_checks
fi
