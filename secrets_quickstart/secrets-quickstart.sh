#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.




docker swarm init
# Remove existing stack deployment
echo "Removing existing secrets_test_stack"
docker stack rm secrets_test
# Wait for 'docker stack rm' to finish (see https://github.com/moby/moby/issues/30942)
limit=15
until [ -z "$(docker service ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done

limit=15
until [ -z "$(docker network ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done

# Remove existing secrets
echo "Removing existing secrets"
docker secret rm FLOWMACHINE_FLOWDB_PASSWORD
docker secret rm FLOWMACHINE_FLOWDB_USER
docker secret rm FLOWAPI_FLOWDB_PASSWORD
docker secret rm FLOWAPI_FLOWDB_USER
docker secret rm FLOWDB_POSTGRES_PASSWORD
docker secret rm cert-flowkit.pem
docker secret rm PRIVATE_JWT_SIGNING_KEY
docker secret rm PUBLIC_JWT_SIGNING_KEY
docker secret rm FLOWAUTH_FERNET_KEY
docker secret rm REDIS_PASSWORD
docker secret rm FLOWAPI_IDENTIFIER
docker secret rm FLOWAUTH_ADMIN_USERNAME
docker secret rm FLOWAUTH_ADMIN_PASSWORD
docker secret rm FLOWAUTH_DB_PASSWORD

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
    -config <( cat $( [[ "Darwin" -eq "$(uname -s)" ]]  && echo /System/Library/OpenSSL/openssl.cnf || echo /etc/ssl/openssl.cnf  ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout cert.key -out cert.pem
cat cert.key cert.pem > cert-flowkit.pem
docker secret create cert-flowkit.pem cert-flowkit.pem

echo "Deploying stack"
docker stack deploy --with-registry-auth -c docker-stack.yml secrets_test
