INGESTION_DB_DOCKER_COMPOSE_FILE ?= docker-compose-ingestion_db.yml
DOCKER_STACK_NAME ?= flowetl_test
DOCKER_STACK_FILE ?= docker-stack-flowkit.yml
LOCAL_DOCKER_REGISTRY ?= 127.0.0.1:5000

#
# Ingestion DB
#

build-and-deploy-ingestion_db: build-ingestion_db deploy-ingestion_db

deploy-ingestion_db:
	docker-compose -f $(INGESTION_DB_DOCKER_COMPOSE_FILE) up -d ingestion_db

build-ingestion_db:
	docker-compose -f $(INGESTION_DB_DOCKER_COMPOSE_FILE) build ingestion_db

shut-down-ingestion_db:
	docker-compose -f $(INGESTION_DB_DOCKER_COMPOSE_FILE) down

#
# FlowETL and FlowDB
#

build-and-deploy-flowkit-stack: build-flowdb build-flowetl push-to-local-registry deploy-flowkit-stack

deploy-flowkit-stack:
	docker stack deploy -c $(DOCKER_STACK_FILE) $(DOCKER_STACK_NAME)

shut-down-flowkit-stack-without-purging-volumes:
	bash ./shut_down_docker_stack.sh $(DOCKER_STACK_NAME)

shut-down-flowkit-stack-and-purge-volumes: shut-down-flowkit-stack-without-purging-volumes purge-flowkit-db-volumes

push-to-local-registry:
	docker push $(LOCAL_DOCKER_REGISTRY)/flowdb:latest
	docker push $(LOCAL_DOCKER_REGISTRY)/flowetl:latest

build-flowdb:
	docker-compose -f $(DOCKER_STACK_FILE) build flowdb

build-flowetl:
	docker-compose -f $(DOCKER_STACK_FILE) build flowetl

connect-flowdb:
	psql "postgresql://flowdb:flowflow@127.0.0.1:${FLOWDB_PORT}/flowdb"

connect-ingestion_db:
	psql "postgresql://ingestion_db:etletl@127.0.0.1:${INGESTION_DB_PORT}/ingestion_db"

connect-flowetl_airflow_db:
	psql "postgresql://flowetl:flowetl@127.0.0.1:${FLOWETL_AIRFLOW_DB_PORT}/flowetl"


purge-flowkit-db-volumes: purge-flowetl_airflow_db-volume purge-flowdb-volume

purge-flowetl_airflow_db-volume:
	docker volume rm $(DOCKER_STACK_NAME)_data_volume_flowetl_airflow_db

purge-flowdb-volume:
	docker volume rm $(DOCKER_STACK_NAME)_data_volume_flowdb

purge-ingestion_db-volume:
	docker volume rm deployment_example_data_volume_ingestion_db
