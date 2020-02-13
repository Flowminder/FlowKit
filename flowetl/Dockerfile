# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWETL
#  -----

FROM puckel/docker-airflow@sha256:30e7cb9744ad367c54ae30a379fa46c9df4ea582bf2fcb96604adfc2760be79a

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_HOME}/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES false
# Turn off api access
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all
ENV AIRFLOW__WEBSERVER__RBAC=True
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

RUN mv /entrypoint.sh /defaultentrypoint.sh
COPY ./entrypoint.sh /entrypoint.sh

# Install FlowETL module

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowetl

COPY Pipfile* ./

RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*

COPY ./flowetl /${SOURCE_TREE}/flowetl
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        python setup.py install && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*

# Needed on $AIRFLOW_HOME so that different users passed through --user
# have the permission to create and use their files for Airflow
#
# When possible, this will get changed to 700 at runtime (uid 0)
RUN chmod -R 777 ${AIRFLOW_HOME}

USER airflow

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
# set default arg for entrypoint
CMD ["webserver"]


