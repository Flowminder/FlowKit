# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.10.5-slim

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
        pipenv run python setup.py install && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
CMD ["pipenv", "run", "flowmachine"]
