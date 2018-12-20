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
 # By setting the variable FLOWDB_SERVICES you can choose which flowdb
# version or versions you'd like to use when running `make up`. Examples:
#
 #     FLOWDB_SERVICES=flowdb_testdata make up
#     FLOWDB_SERVICE=flowdb_synthetic_data make up
#     FLOWDB_SERVICES="flowdb_testdata flowdb_synthetic_data" make up
#
# flowmachine and flowapi will connected to the first flowdb service in the list.

DOCKER_COMPOSE_FILE_DEV ?= docker-compose-dev.yml
export FLOWDB_SERVICES ?= flowdb_testdata
DOCKER_SERVICES ?= flowapi flowmachine redis $(FLOWDB_SERVICES)
export DB_HOST=$(word 1, $(FLOWDB_SERVICES))


all:

up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d $(DOCKER_SERVICES)

down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) down

flowdb_testdata-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d flowdb_testdata

flowdb_testdata-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowdb_testdata

flowdb_synthetic_data-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) up -d --build flowdb_synthetic_data

flowdb_synthetic_data-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE_DEV) rm -f -s -v flowdb_synthetic_data

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
