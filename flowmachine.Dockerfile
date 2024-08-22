# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.8-slim-bullseye@sha256:1e749e00ba22618288f0d16fa610c35d984e5e92b290f39ebd1cda1604220cb5

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowmachine
COPY ./flowmachine/Pipfile* ./
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pip install --no-cache-dir pipenv && pipenv install --clear --deploy && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
COPY . /${SOURCE_TREE}/
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pipenv run pip install --no-deps --no-cache-dir . && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
CMD ["pipenv", "run", "flowmachine"]
# FlowDB has a default role named flowmachine for use with the flowmachine server
# when starting the container with a different user, that user must be in the flowmachine
# role
ENV FLOWMACHINE_FLOWDB_USER=flowmachine
