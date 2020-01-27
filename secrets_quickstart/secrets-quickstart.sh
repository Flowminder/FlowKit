#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -eu
set -o pipefail

# Ensure this docker node is in swarm mode
docker swarm init || true


# Remove existing stack deployment
echo "Removing existing secrets_test_stack"
docker stack rm secrets_test

# Wait for 'docker stack rm' to finish (see https://github.com/moby/moby/issues/30942)
limit=30
until [ -z "$(docker service ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done
if [ "$limit" -lt 0 ]; then
    echo "Not all services in the existing docker stack have been removed."
    echo "Please wait (or try to remove them manually) and run this script again."
    exit 1
fi

limit=30
until [ -z "$(docker network ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done
if [ "$limit" -lt 0 ]; then
    echo "Not all networks in the existing docker stack have been removed."
    echo "Please wait (or try to remove them manually) and run this script again."
    exit 1
fi


# Remove existing secrets
echo "Removing existing secrets"

docker secret rm FLOWMACHINE_FLOWDB_PASSWORD || true
docker secret rm FLOWMACHINE_FLOWDB_USER || true
docker secret rm FLOWAPI_FLOWDB_PASSWORD || true
docker secret rm FLOWAPI_FLOWDB_USER || true
docker secret rm FLOWDB_POSTGRES_PASSWORD || true
docker secret rm cert-flowkit.pem || true
docker secret rm key-flowkit.pem || true
docker secret rm REDIS_PASSWORD || true
docker secret rm FLOWAPI_IDENTIFIER || true
docker secret rm FLOWAUTH_ADMIN_USERNAME || true
docker secret rm FLOWAUTH_ADMIN_PASSWORD || true
docker secret rm FLOWAUTH_DB_PASSWORD || true
docker secret rm PRIVATE_JWT_SIGNING_KEY || true
docker secret rm PUBLIC_JWT_SIGNING_KEY || true
docker secret rm FLOWAUTH_FERNET_KEY || true
docker secret rm SECRET_KEY || true
docker secret rm FLOWAUTH_REDIS_PASSWORD || true
docker secret rm AIRFLOW__CORE__FERNET_KEY || true
docker secret rm AIRFLOW__CORE__SQL_ALCHEMY_CONN || true
docker secret rm FLOWETL_POSTGRES_PASSWORD || true
docker secret rm AIRFLOW_CONN_FLOWDB || true

# Add new secrets

echo "Adding secrets"
# Flowauth
openssl genrsa -out tokens-private-key.key 4096
FLOWAUTH_DB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAUTH_DB_PASSWORD" | docker secret create FLOWAUTH_DB_PASSWORD -
FLOWAUTH_ADMIN_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAUTH_ADMIN_PASSWORD"| docker secret create FLOWAUTH_ADMIN_PASSWORD -
FLOWAUTH_REDIS_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAUTH_REDIS_PASSWORD"| docker secret create FLOWAUTH_REDIS_PASSWORD -
SECRET_KEY=$(openssl rand -base64 64)
echo "$SECRET_KEY"| docker secret create SECRET_KEY -
echo "admin" | docker secret create FLOWAUTH_ADMIN_USERNAME -
pip install cryptography
FLOWAUTH_FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "$FLOWAUTH_FERNET_KEY"| docker secret create FLOWAUTH_FERNET_KEY -


# Flowmachine
FLOWMACHINE_FLOWDB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWMACHINE_FLOWDB_PASSWORD" | docker secret create FLOWMACHINE_FLOWDB_PASSWORD -
echo "flowmachine" | docker secret create FLOWMACHINE_FLOWDB_USER -
echo "flowapi" | docker secret create FLOWAPI_FLOWDB_USER -
REDIS_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$REDIS_PASSWORD"| docker secret create REDIS_PASSWORD -

# FlowDB
FLOWAPI_FLOWDB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAPI_FLOWDB_PASSWORD" | docker secret create FLOWAPI_FLOWDB_PASSWORD -
FLOWDB_POSTGRES_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWDB_POSTGRES_PASSWORD" | docker secret create FLOWDB_POSTGRES_PASSWORD -

# FlowAPI
openssl rsa -pubout -in  tokens-private-key.key -out tokens-public-key.pub
docker secret create PRIVATE_JWT_SIGNING_KEY tokens-private-key.key
docker secret create PUBLIC_JWT_SIGNING_KEY tokens-public-key.pub
echo "flowapi_server" | docker secret create FLOWAPI_IDENTIFIER -
openssl req -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/CN=flow.api" \
    -extensions SAN \
    -config <( \
        cat $(   ( [[ -e /System/Library/OpenSSL/openssl.cnf ]] \
              && echo "/System/Library/OpenSSL/openssl.cnf" ) \
              || ( [[ -e /etc/ssl/openssl.cnf ]] && echo "/etc/ssl/openssl.cnf" ) \
              || ( [[ -e /etc/pki/tls/openssl.cnf ]] && echo "/etc/pki/tls/openssl.cnf" ) ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout key-flowkit.pem -out cert-flowkit.pem
if ! ( [ -e key-flowkit.pem ] && [ -e cert-flowkit.pem ] ); then
    echo "Generation of the SSL certificate failed."
    echo "Please the check the command (in particular the path to the openssl.cnf file) and try again."
    exit
fi
docker secret create cert-flowkit.pem cert-flowkit.pem
docker secret create key-flowkit.pem key-flowkit.pem

# FlowETL
AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "AIRFLOW__CORE__FERNET_KEY"| docker secret create AIRFLOW__CORE__FERNET_KEY -

FLOWETL_POSTGRES_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWETL_POSTGRES_PASSWORD" | docker secret create FLOWETL_POSTGRES_PASSWORD -
AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgres://flowetl:$FLOWETL_POSTGRES_PASSWORD@flowetl_db:5432/flowetl"
echo "AIRFLOW__CORE__SQL_ALCHEMY_CONN" | docker secret create AIRFLOW__CORE__SQL_ALCHEMY_CONN -
echo "postgres://flowdb:$FLOWDB_POSTGRES_PASSWORD@flowdb:5432/flowdb" | docker secret create AIRFLOW_CONN_FLOWDB -
export FLOWETL_HOST_GROUP_ID=$(id -g)
export FLOWETL_HOST_USER_ID=$(id -u)


# Ports
export FLOWDB_HOST_PORT=9000
export FLOWAPI_HOST_PORT=9090
export FLOWETL_HOST_PORT=8080

# Bind mounts

export FLOWDB_HOST_GROUP_ID=$(id -g)
export FLOWDB_HOST_USER_ID=$(id -u)
export FLOWDB_DATA_DIR=./flowdb_pgdata
mkdir $FLOWDB_DATA_DIR || true
export FLOWDB_ETL_DIR=./../flowetl/mounts/files/
export FLOWETL_HOST_DAG_DIR=./../flowetl/mounts/dags/


# Deploy the stack

echo "
===============

Deploying stack
"

docker stack deploy -c flowdb.yml -c flowetl.yml -c flowmachine.yml -c flowapi.yml -c ../flowauth/docker-compose.yml secrets_test

echo "

===============

Deployed with settings:

FlowDB users
flowmachine:$FLOWMACHINE_FLOWDB_PASSWORD
flowapi:$FLOWAPI_FLOWDB_PASSWORD
flowdb:$FLOWDB_POSTGRES_PASSWORD

FlowAuth admin user
admin:$FLOWAUTH_ADMIN_PASSWORD

FlowAuth DB user
flowauth:$FLOWAUTH_DB_PASSWORD

FlowAuth Fernet Key
$FLOWAUTH_FERNET_KEY

FlowAuth secret Key
$SECRET_KEY

FlowAuth redis password
$FLOWAUTH_REDIS_PASSWORD

FlowMachine redis password
$REDIS_PASSWORD

FlowETL Fernet Key
$AIRFLOW__CORE__FERNET_KEY

FlowETL DB connection
$AIRFLOW__CORE__SQL_ALCHEMY_CONN

FlowDB data dir
$FLOWDB_DATA_DIR

===============
"

