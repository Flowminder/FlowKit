# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
#  FLOWETL
#  -----

FROM apache/airflow:2.2.4-python3.8@sha256:a8cba3c4cea6edaf6898c6d086037e85aa408cb794877604260ab95502787806

ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_HOME}/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES False
# Turn off api access
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all
ENV AIRFLOW__WEBSERVER__RBAC=True

# Needed for custom users passed through docker's --user argument, otherwise it's /
ENV HOME ${AIRFLOW_USER_HOME_DIR}

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


