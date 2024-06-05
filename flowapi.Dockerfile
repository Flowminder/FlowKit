# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.12-alpine@sha256:5365725a6cd59b72a927628fdda9965103e3dc671676c89ef3ed8b8b0e22e812

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowapi
COPY ./flowapi/Pipfile* ./
RUN apk update && apk add libzmq && apk add --virtual build-dependencies build-base libffi-dev \
    gcc wget git musl-dev zeromq-dev openssl-dev cargo && \
    pip install --no-cache-dir pipenv==2023.12.1 Cython && pipenv install --clear --deploy && \
    apk del build-dependencies
COPY . /${SOURCE_TREE}/
RUN pipenv run pip install --no-deps --no-cache-dir .
ENV QUART_ENV=production
ENV FLOWAPI_PORT=9090
EXPOSE 80
EXPOSE 443
EXPOSE 9090
CMD ["pipenv", "run", "./start.sh"]
