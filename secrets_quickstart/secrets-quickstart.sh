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
docker secret rm REDIS_PASSWORD || true
docker secret rm FLOWAPI_IDENTIFIER || true
docker secret rm FLOWAUTH_ADMIN_USERNAME || true
docker secret rm FLOWAUTH_ADMIN_PASSWORD || true
docker secret rm FLOWAUTH_DB_PASSWORD || true
docker secret rm PRIVATE_JWT_SIGNING_KEY || true
docker secret rm PUBLIC_JWT_SIGNING_KEY || true

# Add new secrets

echo "Adding secrets"
# Flowauth
openssl genrsa -out tokens-private-key.key 4096
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create FLOWAUTH_DB_PASSWORD -
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create FLOWAUTH_ADMIN_PASSWORD -
echo "admin" | docker secret create FLOWAUTH_ADMIN_USERNAME -
pip install cryptography && python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" | docker secret create FLOWAUTH_FERNET_KEY -


# Flowmachine
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create FLOWMACHINE_FLOWDB_PASSWORD -
echo "flowmachine" | docker secret create FLOWMACHINE_FLOWDB_USER -
echo "flowapi" | docker secret create FLOWAPI_FLOWDB_USER -
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create REDIS_PASSWORD -

# FlowDB
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create FLOWAPI_FLOWDB_PASSWORD -
openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z' | docker secret create FLOWDB_POSTGRES_PASSWORD -

# FlowAPI
openssl rsa -pubout -in  tokens-private-key.key -out tokens-public-key.pub
docker secret create PRIVATE_JWT_SIGNING_KEY tokens-private-key.key
docker secret create PUBLIC_JWT_SIGNING_KEY tokens-public-key.pub
echo "flowapi_server" | docker secret create FLOWAPI_IDENTIFIER -
openssl req -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/CN=flow.api" \
    -extensions SAN \
    -config <( \
        cat $(   ( [[ -e /System/Library/OpenSSL/openssl.cnf ]] && echo "/System/Library/OpenSSL/openssl.cnf" ) \
              || ( [[ -e /etc/ssl/openssl.cnf ]] && echo "/etc/ssl/openssl.cnf" ) \
              || ( [[ -e /etc/pki/tls/openssl.cnf ]] && echo "/etc/pki/tls/openssl.cnf" ) ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout cert.key -out cert.pem
if ! ( [ -e cert.key ] && [ -e cert.pem ] ); then
    echo "Generation of the SSL certificate failed."
    echo "Please the check the command (in particular the path to the openssl.cnf file) and try again."
    exit
fi
cat cert.key cert.pem > cert-flowkit.pem
docker secret create cert-flowkit.pem cert-flowkit.pem

# Deploy the stack

echo "Deploying stack"
docker stack deploy -docker-stack.yml secrets_test
