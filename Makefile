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
# By setting the variable DOCKER_SERVICES you can choose which services
# you'd like to use when running `make up`. Note that at most one flowdb
# service must be specified. Examples:
#
#     DOCKER_SERVICES="flowdb_synthetic_data flowapi flowmachine flowauth flowmachine_query_locker" make up
#     DOCKER_SERVICES="flowdb" make up
#     DOCKER_SERVICES="flowdb_testdata flowetl flowetl_db" make up
#

DOCKER_COMPOSE_FILE ?= docker-compose.yml
DOCKER_COMPOSE_TESTDATA_FILE ?= docker-compose-testdata.yml
DOCKER_COMPOSE_SYNTHETICDATA_FILE ?= docker-compose-syntheticdata.yml
DOCKER_COMPOSE_FILE_BUILD ?= docker-compose-build.yml
DOCKER_COMPOSE_TESTDATA_FILE_BUILD ?= docker-compose-testdata-build.yml
DOCKER_COMPOSE_SYNTHETICDATA_FILE_BUILD ?= docker-compose-syntheticdata-build.yml
DOCKER_SERVICES ?= flowdb flowapi flowmachine flowauth flowmachine_query_locker flowetl flowetl_db worked_examples
DOCKER_COMPOSE_UP = docker-compose -f $(DOCKER_COMPOSE_FILE) \
	-f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE) \
	-f $(DOCKER_COMPOSE_TESTDATA_FILE) \
	-f $(DOCKER_COMPOSE_FILE_BUILD) \
	up -d --build
DOCKER_COMPOSE_UP_NOBUILD = docker-compose -f $(DOCKER_COMPOSE_FILE) \
	-f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE) \
	-f $(DOCKER_COMPOSE_TESTDATA_FILE) \
	up -d
DOCKER_COMPOSE_DOWN = docker-compose -f $(DOCKER_COMPOSE_FILE) \
	-f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE) \
	-f $(DOCKER_COMPOSE_TESTDATA_FILE) \
	rm -f -s -v
DOCKER_COMPOSE_DOWN_ALL = docker-compose -f $(DOCKER_COMPOSE_FILE) \
	-f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE) \
	-f $(DOCKER_COMPOSE_TESTDATA_FILE) \
	down -v
DOCKER_COMPOSE_BUILD = docker-compose -f $(DOCKER_COMPOSE_FILE) \
	-f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE) \
	-f $(DOCKER_COMPOSE_TESTDATA_FILE) \
	-f $(DOCKER_COMPOSE_FILE_BUILD) \
	build

# Check that at most one flowdb service is present in DOCKER_SERVICES
NUM_SPECIFIED_FLOWDB_SERVICES=$(words $(filter flowdb%, $(DOCKER_SERVICES)))
ifneq ($(NUM_SPECIFIED_FLOWDB_SERVICES),0)
  ifneq ($(NUM_SPECIFIED_FLOWDB_SERVICES),1)
    $(error "At most one flowdb service must be specified in DOCKER_SERVICES, but found: $(filter flowdb%, $(DOCKER_SERVICES))")
  endif
endif

all:

up: flowdb-build
	$(DOCKER_COMPOSE_UP) $(DOCKER_SERVICES)

up-no_build:
	$(DOCKER_COMPOSE_UP_NO_BUILD) $(DOCKER_SERVICES)

down:
	$(DOCKER_COMPOSE_DOWN)


# Note: the targets below are repetitive and could be simplified by using
# a pattern rule as follows:
#
#   %: %-build
#       docker-compose -f $(DOCKER_COMPOSE_FILE) up -d --build $*
#
# The reason we are keeping the explicitly spelled-out versions is in order
# to increase discoverability of the available Makefile targets and to enable
# tab-completion of targets (which is not possible when using patterns).

services := flowmachine flowapi flowauth flowdb worked_examples flowdb_testdata flowdb_synthetic_data flowetl flowetl_db
space :=
space +=
$(subst $(space),-up ,$(services)):
	$(DOCKER_COMPOSE_UP) $(subst -up,,$@)

$(subst $(space),-down ,$(services)):
	$(DOCKER_COMPOSE_DOWN) $(patsubst -down,,$@)
	
$(subst $(space),-build ,$(services))::
	$(DOCKER_COMPOSE_BUILD) $(patsubst -build,,$@)
