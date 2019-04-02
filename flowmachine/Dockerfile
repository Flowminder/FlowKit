# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.7

WORKDIR /app
COPY Pipfile* /app/
RUN pip install pipenv && pipenv install --deploy
COPY . /app/
RUN pipenv install --skip-lock .
CMD ["pipenv", "run", "flowmachine"]
ENV LOG_LEVEL=error
ENV FLOWDB_PORT=5432
ENV FLOWDB_HOST=flowdb
ENV FLOWDB_USER=analyst
ENV DEBUG=False
