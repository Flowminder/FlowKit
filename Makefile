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
#     FLOWDB_SERVICES=flowdb_synthetic_data make up
#     FLOWDB_SERVICES="flowdb_testdata flowdb_synthetic_data" make up
#
# flowmachine and flowapi will connected to the first flowdb service in the list.

DOCKER_COMPOSE_FILE ?= docker-compose.yml
DOCKER_COMPOSE_FILE_BUILD ?= docker-compose-build.yml
FLOWDB_SERVICES ?= flowdb_testdata
DOCKER_SERVICES ?= $(FLOWDB_SERVICES) flowapi flowmachine flowauth flowmachine_query_locker
export DOCKER_FLOWDB_HOST=$(word 1, $(FLOWDB_SERVICES))


all:

up: flowdb-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD)  up -d --build $(DOCKER_SERVICES)

up-no_build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d $(DOCKER_SERVICES)

down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) down


# Note: the targets below are repetitive and could be simplified by using
# a pattern rule as follows:
#
#   %-up: %-build
#       docker-compose -f $(DOCKER_COMPOSE_FILE) up -d --build $*
#
# The reason we are keeping the explicitly spelled-out versions is in order
# to increase discoverability of the available Makefile targets and to enable
# tab-completion of targets (which is not possible when using patterns).


flowdb-up: flowdb-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowdb

flowdb-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowdb

flowdb-build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowdb


flowdb_testdata-up: flowdb_testdata-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowdb_testdata

flowdb_testdata-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowdb_testdata

flowdb_testdata-build: flowdb-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowdb_testdata


flowdb_synthetic_data-up: flowdb_synthetic_data-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowdb_synthetic_data

flowdb_synthetic_data-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowdb_synthetic_data

flowdb_synthetic_data-build: flowdb-build
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowdb_synthetic_data


flowmachine-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowmachine

flowmachine-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowmachine

flowmachine-build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowmachine


flowapi-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowapi

flowapi-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowapi

flowapi-build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowapi


flowauth-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) up -d --build flowauth

flowauth-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowauth

flowauth-build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) -f $(DOCKER_COMPOSE_FILE_BUILD) build flowauth


flowmachine_query_locker-up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d flowmachine_query_locker

flowmachine_query_locker-down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) rm -f -s -v flowmachine_query_locker
