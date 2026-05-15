# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM python:3.13-bullseye@sha256:f58f33e0563f2ba81c7afe6259cd912f0c33413da93c75cc3a70a941c17afa8c

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
ENV TINI_VERSION="v0.19.0"
# Pin pipenv: different pipenv versions hash the same Pipfile differently.
# 2024.4.1 is the version that hashes flowmachine/Pipfile to the value
# stored in the current Pipfile.lock; using a newer pipenv produces a
# different hash and breaks `--deploy`. Keep this in sync with
# .circleci/config.yml's install_flowmachine_deps job.
#
# Note: do NOT name this ARG `PIPENV_VERSION` — pipenv treats that as its
# `--version` boolean flag and fails on any non-boolean value.
ARG PIPENV_PIN=2024.4.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
WORKDIR /${SOURCE_TREE}/flowmachine
COPY ./flowmachine/Pipfile* ./
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pip install --no-cache-dir "pipenv==${PIPENV_PIN}" && pipenv install --clear --deploy && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/*
COPY . /${SOURCE_TREE}/
RUN apt-get update && \
        apt-get install -y --no-install-recommends git && \
        pipenv run pip install --no-deps --no-cache-dir . && \
        apt-get -y remove git && \
        apt purge -y --auto-remove && \
        rm -rf /var/lib/apt/lists/* 
ENTRYPOINT ["/tini", "--", "pipenv", "run"]
CMD ["flowmachine"]
# FlowDB has a default role named flowmachine for use with the flowmachine server
# when starting the container with a different user, that user must be in the flowmachine
# role
ENV FLOWMACHINE_FLOWDB_USER=flowmachine
