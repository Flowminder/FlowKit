FROM python:3.7-slim

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
COPY . /${SOURCE_TREE}/autoflow/

ENV AUTOFLOW_DB_URI="sqlite:////tmp/test.db"
ENV AUTOFLOW_INPUTS_DIR=/mounts/inputs
ENV AUTOFLOW_OUTPUTS_DIR=/mounts/outputs
ENV PREFECT__USER_CONFIG_PATH=/${SOURCE_TREE}/autoflow/config/config.toml
ENV PREFECT__ASCIIDOC_TEMPLATE_PATH=/${SOURCE_TREE}/autoflow/config/asciidoc_extended.tpl

RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get upgrade -yqq git \
    && apt-get install -yqq pandoc ruby  gcc \
    && gem install asciidoctor \
    && gem install asciidoctor-pdf --pre \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

WORKDIR /${SOURCE_TREE}/autoflow
RUN pip install -U pip && pip install .[postgres,examples]

CMD ["python", "-m", "autoflow"]
