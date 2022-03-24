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


rand_string_secrets=(
  FLOWAUTH_DB_PASSWORD
  FLOWAUTH_ADMIN_PASSWORD
  FLOWAUTH_REDIS_PASSWORD
  FLOWMACHINE_FLOWDB_PASSWORD
  REDIS_PASSWORD
  FLOWAPI_FLOWDB_PASSWORD
  FLOWDB_POSTGRES_PASSWORD
  FLOWETL_POSTGRES_PASSWORD
  FLOWETL_CELERY_PASSWORD
  FLOWETL_REDIS_PASSWORD
  )

rand_int_secrets=(
  SECRET_KEY
)

declare -A hard_secrets
hard_secrets=(
  [FLOWATUH_ADMIN_USERNAME]="admin"
  [FLOWMACHINE_FLOWDB_USER]="flowmachine"
  [FLOWAPI_FLOWDB_USER]="flowapi"
  [FLOWAPI_IDENTIFIER]="flowapi_server"
  [FLOWETL_POSTGRES_USER]="flowetl"
  [FLOWETL_CELERY_USER]="flowetl"
)

fernet_secrets=(
  FLOWAUTH_FERNET_KEY
  AIRFLOW__CORE__FERNET_KEY
)

other_secrets=(
  cert-flowkit.pem
  key-flowkit.pem
  PRIVATE_JWT_SIGNING_KEY
  PUBLIC_JWT_SIGNING_KEY
)

all_secrets=(
  "${rand_string_secrets[*]}"
  "${rand_int_secrets[*]}"
  "${!hard_secrets[*]}"
  "${fernet_secrets[*]}"
  "${other_secrets[*]}"
)

# Remove existing secrets
echo "Removing existing secrets..."

for secret in ${all_secrets[*]} ; do
    docker secret rm $secret || true
done

echo "Generating random string secrets..."
for secret_name in ${rand_string_secrets[*]} ; do
  echo "Generating $secret_name"
  this_pass=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
  echo "$this_pass" | docker secret create $secret_name -
done

echo "Generating random int secrets..."
for secret_name in ${rand_int_secrets[*]} ; do
  echo "Generating $secret_name"
  this_pass=$(openssl rand -base64 64)
  echo "$this_pass" | docker secret create $secret_name -
done

echo "Setting hard-coded secrets..."
for secret_name in ${!hard_secrets[*]} ; do
  secret_val=${hard_secrets[${secret_name}]}
  echo "Setting $secret_name to $secret_val"
  echo "$secret_val" | docker secret create $secret_name -
done

echo "Setting up Fernet key generation..."
pip install cryptography

echo "Generating Fernet keys..."
for secret_name in ${fernet_secrets[*]} ; do
  echo "Generating $secret_name"
  f_key=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  echo "$f_key" | docker secret create $secret_name -
done

echo "Generating Flowauth RSA key"
openssl genrsa -out tokens-private-key.key 4096

echo "Generating FlowAPI public-private key pair"
openssl rsa -pubout -in  tokens-private-key.key -out tokens-public-key.pub
docker secret create PRIVATE_JWT_SIGNING_KEY tokens-private-key.key
docker secret create PUBLIC_JWT_SIGNING_KEY tokens-public-key.pub

echo "Generating FlowAPI SSL cert"
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


echo "Secret gen complete"

export FLOWETL_HOST_GROUP_ID=$(id -g)
export FLOWETL_HOST_USER_ID=$(id -u)


# Ports
export FLOWDB_HOST_PORT=9000
export FLOWAPI_HOST_PORT=9090
export FLOWETL_HOST_PORT=8080
export REDIS_HOST_PORT=6379

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

