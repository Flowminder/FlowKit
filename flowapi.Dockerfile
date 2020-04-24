# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.8-alpine

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/
COPY ./flowapi/Pipfile* ./
RUN apk update && apk add libzmq && apk add --virtual build-dependencies build-base libffi-dev \
    gcc wget git musl-dev zeromq-dev openssl-dev && \
    pip install --no-cache-dir pipenv Cython && pipenv install --clear --deploy && \
    apk del build-dependencies
COPY ./* /${SOURCE_TREE}/
RUN pipenv run python setup.py install
ENV QUART_ENV=production
CMD ["pipenv", "run", "./start.sh"]
