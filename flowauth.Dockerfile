# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM node as builder

COPY flowauth/frontend /
RUN npm install --production
RUN PUBLIC_URL=/static npm run-script build

FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
COPY flowauth/Pipfile* .
# Install dependencies required for argon crypto & psycopg2
RUN apk update && apk add --no-cache --virtual build-dependencies build-base postgresql-dev gcc python3-dev musl-dev \
    libressl-dev libffi-dev mariadb-connector-c-dev && \
    pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system && \
    apk del build-dependencies && \
    apk add --no-cache libpq mariadb-connector-c # Required for psycopg2 & mysqlclient
ENV STATIC_PATH /app/static
ENV STATIC_INDEX 1
ENV FLASK_APP flowauth
ENV PIPENV_COLORBLIND=1
WORKDIR /app/static
COPY --from=builder /build .
ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowauth
COPY flowauth/backend .
RUN python setup.py install && mv uwsgi.ini /app
WORKDIR /app

ENV FLOWAUTH_CACHE_BACKEND FILE
ENV FLOWAUTH_CACHE_FILE /dev/shm/flowauth_last_used_cache