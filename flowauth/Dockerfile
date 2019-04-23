# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM node as builder

COPY frontend /
RUN npm install --production
RUN PUBLIC_URL=/static npm run-script build

FROM tiangolo/uwsgi-nginx-flask:python3.6-alpine3.8
ENV STATIC_PATH /app/static
ENV STATIC_INDEX 1
ENV FLASK_APP flowauth
WORKDIR /app/static
COPY --from=builder /build .
WORKDIR /app
COPY backend .
COPY Pipfile .
COPY Pipfile.lock .
RUN pip install --no-cache-dir pipenv && pipenv install --clear --deploy --system && pip install .