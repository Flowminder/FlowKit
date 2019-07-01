# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWETL
#  -----

FROM greenape/docker-airflow

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_HOME}/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES false
USER root
WORKDIR /usr/src/deps

# Needed for custom users passed through docker's --user argument, otherwise it's /
ENV HOME ${AIRFLOW_HOME}

# Use "nss_wrapper" to fake "/etc/passwd" and "/etc/group" for Airflow when
# running under a different user for development purposes. Related uses:
# https://github.com/docker-library/postgres/issues/359
# https://cwrap.org/nss_wrapper.html
RUN set -eux; \
        apt-get update; \
        apt-get install -y --no-install-recommends libnss-wrapper; \
        rm -rf /var/lib/apt/lists/*

ENV LD_PRELOAD /usr/lib/libnss_wrapper.so
ENV NSS_WRAPPER_PASSWD /tmp/nsspasswd
ENV NSS_WRAPPER_GROUP /tmp/nssgroup

RUN mv /entrypoint.sh /defaultentrypoint.sh
COPY ./entrypoint.sh /entrypoint.sh

COPY ./etl /opt/etl
RUN pip install /opt/etl

USER airflow
COPY --chown=airflow:airflow ./dags/* ${AIRFLOW_HOME}/src/

# Needed on $AIRFLOW_HOME so that different users passed through --user
# have the permission to create and use their files for Airflow
RUN chmod -R 777 ${AIRFLOW_HOME}

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint


