# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWETL
#  -----

FROM apache/airflow:2.4.2-python3.8@sha256:ea17c4d9ad3d5b9259699197918b689c47388745a1433d59dd2b1acad83c031a

ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_HOME}/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES False
# Turn off api access
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all
ENV AIRFLOW__WEBSERVER__RBAC=True

# Needed for custom users passed through docker's --user argument, otherwise it's /
ENV HOME ${AIRFLOW_USER_HOME_DIR}

# Need pg_config installed before we can install apache-airflow-providers-postgres, which no longer has psycopg2-binary as a dependency
# Note: apache-airflow-providers-postgres is already installed in the image (as is airflow and all its dependencies).
# We shouldn't need to install them all again (via 'pipenv install'), but we'd then need to find a different way to
# ensure that non-containerised tests run against the same dependencies as are installed in the Docker image.
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb https://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install FlowETL module

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowetl

COPY --chown=airflow . /${SOURCE_TREE}/


RUN pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system
RUN cd flowetl && python setup.py install --prefix /home/airflow/.local


WORKDIR ${AIRFLOW_HOME}
COPY ./flowetl/entrypoint.sh /flowetl_entry.sh
COPY ./flowetl/init.sh /init.sh
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/flowetl_entry.sh"]
# set default arg for entrypoint
EXPOSE 80
EXPOSE 8080
ENV FLOWETL_PORT=8080

CMD ["webserver"]


