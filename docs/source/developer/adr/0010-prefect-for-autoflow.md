# Prefect for workflow definition and execution in AutoFlow

Date: 22 November 2019

# Status

Pending

## Context

The first prototype of AutoFlow used [Apache Airflow](https://airflow.apache.org/) (as used in FlowETL) to define and execute workflows. However, this proved to be problematic in some respects - Airflow has limited support for parametrising DAG runs and sharing data between tasks, and re-running a DAG for an execution date for which it has already run is complicated.

[Prefect Core](https://docs.prefect.io/) is an alternative open-source workflow engine, which allows DAGs to be parametrised and run simultaneously for multiple sets of parameters, and allows data exchange between tasks. Prefect also allows the creation of dynamically-generated tasks mapped over the outputs from running another task, which makes it easier for AutoFlow to spawn multiple runs of a workflow when the sensor finds multiple days of data for which the workflow has not previously run.

## Decision

AutoFlow will use Prefect to define and run workflows.

## Consequences

The process of parametrising and dynamically mapping workflow runs is simpler than it would be with Airflow.

Unlike Airflow, Prefect Core is not a full workflow management system - it provides the functionality for defining workflows and running them individually, but the full workflow orchestration and monitoring system is left to the proprietary Prefect Cloud platform. As a result, AutoFlow cannot benefit from a UI such as Airflow provides, and if in the future we want AutoFlow to run multiple sensor workflows we will need to write our own code to do this concurrently.
