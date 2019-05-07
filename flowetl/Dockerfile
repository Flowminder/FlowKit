FROM puckel/docker-airflow

ARG AIRFLOW_HOME=/usr/local/airflow
USER root
WORKDIR /usr/src/deps

# This allows us to start airflow as a specified UID/GID - shouldn't really use in 
# production so should perhaps rebuild for specific server when used in production
RUN USER=airflow && \
        GROUP=airflow && \
        curl -SsL https://github.com/boxboat/fixuid/releases/download/v0.4/fixuid-0.4-linux-amd64.tar.gz | tar -C /usr/local/bin -xzf - && \
        chown root:root /usr/local/bin/fixuid && \
        chmod 4755 /usr/local/bin/fixuid && \
        mkdir -p /etc/fixuid && \
        printf "user: $USER\ngroup: $GROUP\n" > /etc/fixuid/config.yml

RUN mv /entrypoint.sh /defaultentrypoint.sh
COPY entrypoint.sh /entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint


