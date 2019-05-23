# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM node as builder

COPY frontend /
RUN npm install --production
RUN PUBLIC_URL=/static npm run-script build


FROM tiangolo/uwsgi-nginx-flask:python3.6-alpine3.8
COPY Pipfile .
COPY Pipfile.lock .
# Install dependencies required for argon crypto & psycopg2
RUN apk update && apk add --virtual build-dependencies build-base postgresql-dev gcc python3-dev musl-dev \
    libressl-dev libffi-dev && \
    pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system && \
    apk del build-dependencies && \
    apk add libpq # Required for psycopg2
ENV STATIC_PATH /app/static
ENV STATIC_INDEX 1
ENV FLASK_APP flowauth
ENV PIPENV_COLORBLIND=1
WORKDIR /app/static
COPY --from=builder /build .
WORKDIR /app
COPY backend .
RUN pip install --no-cache-dir .