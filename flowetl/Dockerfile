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

# GOSU allows drop-down from root privileges when running the airflow binary
ENV GOSU_VERSION 1.11
RUN set -x \
        && apt-get update && apt-get install -y --no-install-recommends ca-certificates wget gnupg dirmngr && rm -rf /var/lib/apt/lists/* \
        && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
        && wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
        && export GNUPGHOME="$(mktemp -d)" \
        && gpg --batch --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
        && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
        && { command -v gpgconf > /dev/null && gpgconf --kill all || :; } \
        && rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc \
        && chmod +x /usr/local/bin/gosu \
        && gosu nobody true \
        && apt-get purge -y --auto-remove ca-certificates wget gnupg dirmngr

RUN mv /entrypoint.sh /defaultentrypoint.sh
COPY ./entrypoint.sh /entrypoint.sh

COPY ./etl /opt/etl
RUN pip install /opt/etl

COPY --chown=airflow:airflow ./dags/* ${AIRFLOW_HOME}/src/
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["gosu", "airflow", "/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint


