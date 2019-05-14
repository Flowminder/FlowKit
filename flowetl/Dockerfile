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

# This allows us to start airflow as a specified UID/GID - shouldn't really use in
# production so should perhaps rebuild for specific server when used in production
RUN USER=airflow && \
        GROUP=airflow && \
        curl -SsL https://github.com/boxboat/fixuid/releases/download/v0.4/fixuid-0.4-linux-amd64.tar.gz | tar -C /usr/local/bin -xzf - && \
        chown root:root /usr/local/bin/fixuid && \
        chmod 4755 /usr/local/bin/fixuid && \
        mkdir -p /etc/fixuid && \
        printf "user: $USER\ngroup: $GROUP\n" > /etc/fixuid/config.yml

RUN mv /entrypoint.sh /defaultentrypoint.sh
COPY ./entrypoint.sh /entrypoint.sh

COPY ./etl /opt/etl
RUN pip install /opt/etl

USER airflow
COPY --chown=airflow:airflow ./dags/* ${AIRFLOW_HOME}/src/
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint


