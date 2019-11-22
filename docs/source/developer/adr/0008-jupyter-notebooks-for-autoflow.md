# Jupyter notebooks for automated notebook execution and report generation in AutoFlow

Date: 22 November 2019

# Status

Pending

## Context

AutoFlow aims to provide a method for automating workflows involving FlowAPI queries. As executable documents, [Jupyter notebooks](https://jupyter.org/) provide a medium for users to define workflows (using FlowClient to communicate with FlowAPI) in a familiar analysis environment, and supply these workflows to AutoFlow to be scheduled and executed. Additionally, Jupyter's markdown cells and inline display of outputs (including images and markdown) make Jupyter notebooks suitable candidates for producing static reports (using [nbconvert](https://nbconvert.readthedocs.io/en/latest/)).

Another advantage of using Jupyter notebooks for automated workflows is that if any errors occur during execution, these errors will be displayed inline within the notebook, aiding debugging.

With the decision to automate running notebooks comes the need to parametrise the notebooks at execution time (otherwise we would be repeatedly running an identical notebook and getting the same output). AutoFlow will use [Papermill](https://papermill.readthedocs.io) to parametrise and execute Jupyter notebooks. Additionally, [scrapbook](https://nteract-scrapbook.readthedocs.io) (formerly a part of the papermill library) can be used to persist data in a notebook so that it can be re-used in a later notebook. This allows for building workflows from multiple notebooks, for example to produce a daily PDF report that compiles results from multiple separate analyses.

## Decision

AutoFlow will use Jupyter notebooks as the central user-defined components of workflows (both for defining queries to be run and for producing the content of output reports), and will use Papermill to parametrise and execute the notebooks. Scrapbook will also be installed in an AutoFlow deployment, so that users can share data between several notebooks in a workflow.

## Consequences

This decision should allow a lot of flexibility in AutoFlow usage, since anything that can be done in a Jupyter notebook can be automated with AutoFlow. Analysts will typically be familiar with Jupyter notebooks already, so the process of prototyping a workflow before eventually automating it with AutoFlow should be fairly user-friendly.

We should remain alert to potential security consequences of this decision, since allowing users to provide their own Jupyter notebooks in principle allows them to execute arbitrary code within the AutoFlow container.
