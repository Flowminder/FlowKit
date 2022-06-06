FROM python:3.10.4-slim

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
COPY . /${SOURCE_TREE}/

ENV AUTOFLOW_DB_URI="sqlite:////tmp/test.db"
ENV AUTOFLOW_INPUTS_DIR=/mounts/inputs
ENV AUTOFLOW_OUTPUTS_DIR=/mounts/outputs
ENV PREFECT__USER_CONFIG_PATH=/${SOURCE_TREE}/autoflow/config/config.toml
ENV PREFECT__ASCIIDOC_TEMPLATE_PATH=/${SOURCE_TREE}/autoflow/config/asciidoc_extended.tpl

WORKDIR /${SOURCE_TREE}/autoflow
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get upgrade -yqq git \
    && apt-get install -yqq pandoc ruby gcc \
    && gem install bundler \
    && bundle install \
    && apt-get remove -y gcc \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

RUN apt-get update -yqq \
    && apt-get install -yqq gcc g++ \
    && pip install -U pip && pip install .[postgres,examples] \
    && apt-get remove -y gcc g++ \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base


CMD ["python", "-m", "autoflow"]
