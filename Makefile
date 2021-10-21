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
#     make up DOCKER_SERVICES="flowdb_synthetic_data flowapi flowmachine flowauth flowmachine_query_locker"
#     make up DOCKER_SERVICES="flowdb"
#     make up DOCKER_SERVICES="flowdb_testdata flowetl flowetl_db"
#

DOCKER_COMPOSE_FILE ?= docker-compose.yml
DOCKER_COMPOSE_AUTOFLOW_FILE ?= docker-compose.autoflow.yml
DOCKER_COMPOSE_TESTDATA_FILE ?= docker-compose-testdata.yml
DOCKER_COMPOSE_SYNTHETICDATA_FILE ?= docker-compose-syntheticdata.yml
DOCKER_COMPOSE_FILE_BUILD ?= docker-compose-build.yml
DOCKER_COMPOSE_TESTDATA_FILE_BUILD ?= docker-compose-testdata-build.yml
DOCKER_COMPOSE_SYNTHETICDATA_FILE_BUILD ?= docker-compose-syntheticdata-build.yml
DOCKER_SERVICES ?= flowdb flowapi flowmachine flowauth flowmachine_query_locker flowetl flowetl_db worked_examples
DOCKER_SERVICES_TO_START = $(patsubst flowdb%,flowdb,$(DOCKER_SERVICES))
services := flowmachine flowmachine_query_locker flowapi flowauth flowdb worked_examples flowdb_testdata flowdb_synthetic_data flowetl flowetl_db autoflow
space :=
space +=
DOCKER_COMPOSE := docker-compose -f $(DOCKER_COMPOSE_FILE)
FLOWDB_SERVICE := $(filter flowdb%, $(DOCKER_SERVICES))

# Add autoflow if specified
NUM_AUTOFLOW=$(words $(filter autoflow%, $(DOCKER_SERVICES)))
ifeq ($(NUM_AUTOFLOW),1)
    DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_AUTOFLOW_FILE)
endif

# Check that at most one flowdb service is present in DOCKER_SERVICES
NUM_SPECIFIED_FLOWDB_SERVICES=$(words $(filter flowdb%, $(DOCKER_SERVICES)))
ifneq ($(NUM_SPECIFIED_FLOWDB_SERVICES),0)
  ifneq ($(NUM_SPECIFIED_FLOWDB_SERVICES),1)
	$(error "At most one flowdb service must be specified in DOCKER_SERVICES, but found: $(filter flowdb%, $(DOCKER_SERVICES))")
  endif
endif

up : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_FILE_BUILD)

ifeq ($(FLOWDB_SERVICE), flowdb_testdata)
 up up-no_build : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_TESTDATA_FILE)
 up : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_TESTDATA_FILE_BUILD)
endif
ifeq ($(FLOWDB_SERVICE), flowdb_synthetic_data)
 up up-no_build : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE)
 up : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE_BUILD)
endif

# Only build flowdb if a flowdb service is requested
ifeq ($(words $(FLOWDB_SERVICE)), 1)
    FLOWDB_BUILD = flowdb-build
endif

all:

up: $(FLOWDB_BUILD)
	$(DOCKER_COMPOSE) up --build -d $(DOCKER_SERVICES_TO_START)

up-no_build:
	$(DOCKER_COMPOSE) up -d $(DOCKER_SERVICES_TO_START)

down:
	$(DOCKER_COMPOSE) down -v


# Per-service targets
# While we could define these using a % pattern, those aren't tab-completable



flowdb_testdata-up flowdb_testdata-down: DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_TESTDATA_FILE)
flowdb_synthetic_data-up flowdb_synthetic_data-down : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE)

%-build: DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_FILE_BUILD)

flowdb_testdata-build flowdb_synthetic_data-build: flowdb-build

flowdb_testdata-build : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_TESTDATA_FILE_BUILD)
flowdb_synthetic_data-build : DOCKER_COMPOSE += -f $(DOCKER_COMPOSE_SYNTHETICDATA_FILE_BUILD)


$(services:=-up):
	$(DOCKER_COMPOSE) up -d $(subst _synthetic_data,,$(subst _testdata,,$(@:-up=)))

$(services:=-down):
	$(DOCKER_COMPOSE) rm -f -s -v $(subst _synthetic_data,,$(subst _testdata,,$(@:-down=)))
	
$($(filter-out flowetl_db flowmachine_query_locker, services):=-build):
	$(DOCKER_COMPOSE) build $(subst _synthetic_data,,$(subst _testdata,,$(@:-build=)))
