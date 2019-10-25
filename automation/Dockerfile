FROM python:3.7-slim

ARG SOURCE_VERSION=0+unknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
COPY . /${SOURCE_TREE}/automation/

ENV AUTOMATION_DB_URI="sqlite:////tmp/test.db"
ENV AUTOMATION_CONFIG_DIR=/${SOURCE_TREE}/automation/config
ENV PREFECT__USER_CONFIG_PATH=${AUTOMATION_CONFIG_DIR}/config.toml

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

WORKDIR /${SOURCE_TREE}/automation
RUN pip install -U pip && pip install .[postgres,examples]

CMD ["python", "-m", "automation"]
