# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.7

COPY Pipfile* /app/
RUN pip install pipenv && cd app && pipenv install --deploy
COPY . /app/
WORKDIR /app
RUN pipenv run pip install .
CMD ["pipenv", "run", "flowmachine"]
ENV LOG_LEVEL=error
ENV DB_PORT=5432
ENV DB_HOST=flowdb
ENV DB_USER=analyst
ENV DB_PASS=flowflow
ENV DEBUG=False