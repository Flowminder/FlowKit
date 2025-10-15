# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.14-alpine@sha256:8373231e1e906ddfb457748bfc032c4c06ada8c759b7b62d9c73ec2a3c56e710

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
WORKDIR /${SOURCE_TREE}/flowapi
COPY ./flowapi/Pipfile* ./
RUN apk update && apk add libzmq && apk add --virtual build-dependencies build-base libffi-dev \
    gcc wget git musl-dev zeromq-dev openssl-dev cargo && \
    pip install --no-cache-dir pipenv==2021.5.29 Cython && pipenv install --clear --deploy && \
    apk del build-dependencies
COPY . /${SOURCE_TREE}/
RUN pipenv run pip install --no-deps --no-cache-dir .
ENV QUART_ENV=production
ENV FLOWAPI_PORT=9090
EXPOSE 80
EXPOSE 443
EXPOSE 9090
CMD ["pipenv", "run", "./start.sh"]
