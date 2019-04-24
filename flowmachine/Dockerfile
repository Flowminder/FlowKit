# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.7-slim

WORKDIR /app
COPY Pipfile* /app/
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pip install --no-cache-dir pipenv && pipenv install --clear --deploy && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
COPY . /app/
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pipenv install --clear --skip-lock . && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
CMD ["pipenv", "run", "flowmachine"]
