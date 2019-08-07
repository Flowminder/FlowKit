#!/usr/bin/env bash

# This script waits for services and networks associated with a given docker stack
# to be removed.
#
# References:
#  - https://github.com/moby/moby/issues/30942
#  - https://github.com/moby/moby/issues/30942#issuecomment-444611989

set -euo pipefail

DOCKER_STACK_NAME=${1:?Must provide name of docker stack as first argument}

echo "Attempting to shut down docker stack: '${DOCKER_STACK_NAME}'"
docker stack rm $DOCKER_STACK_NAME || true


limit=120
echo -n "Waiting up to $limit seconds for services to be removed ..."
until { [ $limit -lt 0 ] && exit_status=1; } || { ( [ -z "$(docker service ls --filter label=com.docker.stack.namespace=$DOCKER_STACK_NAME -q)" ] ) && exit_status=0; }; do
  echo -n "."
  sleep 1;
  limit="$((limit-1))";
done
if [ "$exit_status" -ne 0 ]; then
  echo ""
  echo "Some services could not be removed, exiting."
  exit $exit_status;
fi
echo "Done."


limit=120
echo -n "Waiting up to $limit seconds for networks to be removed ..."
until { [ $limit -lt 0 ] && exit_status=1; } || { ( [ -z "$(docker network ls --filter label=com.docker.stack.namespace=$DOCKER_STACK_NAME -q)" ] ) && exit_status=0; }; do
  echo -n "."
  sleep 1;
  limit="$((limit-1))";
done
if [ "$exit_status" -ne 0 ]; then
  echo ""
  echo "Some networks could not be removed, exiting."
  exit $exit_status;
fi
echo "Done."


echo "Docker stack '${DOCKER_STACK_NAME}' was successfully shut down."
