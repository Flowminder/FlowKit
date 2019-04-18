# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.7-alpine

WORKDIR /app
COPY Pipfile* /app/
RUN apk update && apk add libzmq && apk add --virtual build-dependencies build-base gcc wget git musl-dev zeromq-dev && \
    pip install --no-cache-dir pipenv Cython && pipenv install --clear --deploy && \
    apk del build-dependencies
COPY . /app/
RUN pipenv install --clear --skip-lock .
CMD ["pipenv", "run", "flowmachine"]
