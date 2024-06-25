#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


set -euo pipefail
export PGUSER="$POSTGRES_USER"
SKIP_TEST_QA_CHECK=${SKIP_TEST_QA_CHECK:-"false"}

#
#  Ingest test data.
#

DIR="/docker-entrypoint-initdb.d/sql/testdata"
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

# &{VAR,,} should lowercase the variable on interpolation
if [ "${SKIP_TEST_QA_CHECK,,}" != "true" ]; then
   echo "Running qa checks in /docker-entrypoint-initdb.d/qa_checks"
   cd /docker-entrypoint-initdb.d
   pipenv run python run_qa_checks.py qa_checks
fi
