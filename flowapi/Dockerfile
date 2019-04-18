# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.7-alpine

WORKDIR /flowapi
COPY Pipfile* /
RUN apk update && apk add --virtual build-dependencies build-base gcc wget git libzmq musl-dev zeromq-dev && \
    pip install pipenv Cython && pipenv install --deploy && \
    apk del build-dependencies
COPY . /flowapi/
RUN pipenv install --skip-lock .
ENV QUART_ENV=production
CMD ["pipenv", "run", "./start.sh"]
