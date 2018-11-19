#
# Makefile for Flowkit.
#
# This is not needed to actually build anything in Flowkit, but it
# contains convenience targets to spin up or tear down individual
# Flowkit docker containers.
#
# For instance, run `make up` to spin up all docker containers, or
# `make flowapi-down` to tear down the docker container for flowapi
# only.
#
# By setting the variable FLOWDB_SERVICE you can choose which flowdb
# version you'd like to use when running `make up`. Examples:
#
#     FLOWDB=flowdb-testdata make up
#     FLOWDB=flowdb-synthetic-data make up
#

DOCKER_COMPOSE_FILE_DEV ?= docker-compose-dev.yml
FLOWDB_SERVICES ?= flowdb-testdata
DOCKER_SERVICES ?= flowapi flowmachine redis $(FLOWDB_SERVICES)

all:

up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d $(DOCKER_SERVICES)

down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) down

flowdb-testdata-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d flowdb-testdata

flowdb-testdata-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowdb-testdata

flowdb-synthetic-data-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d --build flowdb-synthetic-data

flowdb-synthetic-data-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowdb-synthetic-data

flowapi-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d --build flowapi

flowapi-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowapi

flowmachine-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d --build flowmachine

flowmachine-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowmachine

redis-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d redis

redis-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v redis

remove-flowdb-volume:
	docker volume rm flowkit_data_volume_flowdb
