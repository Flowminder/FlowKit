# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWETL
#  -----

FROM apache/airflow:2.0.0-python3.8

ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_HOME}/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES false
# Turn off api access
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all
ENV AIRFLOW__WEBSERVER__RBAC=True
USER root

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



# Install FlowETL module

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowetl

COPY . /${SOURCE_TREE}/

RUN apt-get update && \
        apt-get install -y --no-install-recommends git build-essential && \
        pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system && \
        apt-get -y remove git build-essential && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        cd flowetl && \
        python setup.py install && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/* && \
        mv /${SOURCE_TREE}/flowetl/entrypoint.sh /

# Deal with old bind mounts to /usr/local/airflow

RUN ln -s /opt/airflow /usr/local/airflow

# Needed on $AIRFLOW_HOME so that different users passed through --user
# have the permission to create and use their files for Airflow
#
# When possible, this will get changed to 700 at runtime (uid 0)
RUN chmod -R 777 ${AIRFLOW_HOME}
RUN  apt-get update && \
        apt-get install -y --no-install-recommends authbind && \
        touch /etc/authbind/byport/80  && \
        chmod 777 /etc/authbind/byport/80 && \
        chown airflow /etc/authbind/byport/80 && \
        rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR ${AIRFLOW_HOME}
# Fix airflow hitting troubles when running getpass.getuser()
ENV USER airflow
ENTRYPOINT ["/entrypoint.sh"]
# set default arg for entrypoint
EXPOSE 80
EXPOSE 8080
ENV FLOWETL_PORT=8080

CMD ["webserver"]


