#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



docker login
docker swarm init
# Remove existing stack deployment
echo "Removing existing secrets_test_stack"
docker stack rm secrets_test
# Remove existing secrets
echo "Removing existing secrets"
docker secret rm FM_DB_PASS
docker secret rm FM_DB_USER
docker secret rm API_DB_PASS
docker secret rm FLOWAPI_DB_USER
docker secret rm POSTGRES_PASSWORD_FILE
docker secret rm cert-flowkit.pem
docker secret rm JWT_SECRET_KEY
docker secret rm REDIS_PASSWORD_FILE
echo "Adding secrets"
openssl rand -base64 16 | docker secret create FM_DB_PASS -
echo "fm" | docker secret create FM_DB_USER -
echo "api" | docker secret create FLOWAPI_DB_USER -
openssl rand -base64 16 | docker secret create API_DB_PASS -
openssl rand -base64 16 | docker secret create POSTGRES_PASSWORD_FILE -
openssl rand -base64 16 | docker secret create REDIS_PASSWORD_FILE -
openssl req -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/CN=flow.api" \
    -extensions SAN \
    -config <( cat $( [[ "Darwin" -eq "$(uname -s)" ]]  && echo /System/Library/OpenSSL/openssl.cnf || echo /etc/ssl/openssl.cnf  ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout cert.key -out cert.pem
cat client_cert.key cert.pem > cert-flowkit.pem
docker secret create cert-flowkit.pem cert-flowkit.pem
echo "secret" | docker secret create JWT_SECRET_KEY -
echo "Deploying stack"
docker stack deploy --with-registry-auth -c docker-stack.yml secrets_test
