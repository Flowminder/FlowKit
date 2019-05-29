# Pipenv for package / dependency management and virtual environments

Date: 21 June 2018


## Status

Accepted.


## Context

The discussion about which package & dependency manager to use has come up
repeatedly over time. The main contenders are `pip`, `pipenv` and `conda`.
All of them have been used (and even been mixed) in different Flowminder
projects and deployments.

Each of them has different pros/cons depending on the context. Here is a
brief summary given our context.

(a) pip:

- Pros: pure Python, no extra dependencies, [PyPA recommended](https://packaging.python.org/guides/tool-recommendations/)
- Cons: installing packages can be a pain if they involve C extensions or other compiled code (and no wheels are provided); even more so for packages that depend on external libraries

(b) [pipenv](https://docs.pipenv.org/):


- Pros: same as `pip`; comes with support for managing virtual environments; improved dependency management over pure `pip`
- Cons: same as `pip`


&#40;c) [conda](https://conda.io/miniconda.html)

- Pros: supports packaging of non-Python dependencies (e.g. third-party libraries), which is advantageous if the user doesn't have full control over the system (e.g. installation in a non-admin environment)
- Cons: introduces a third-party dependency; not always clear which conda channels provide which packages


## Decision

We will use pipenv for dependency management.


## Consequences

Pipenv is not intended for the management/installation of non-Python dependencies.
Therefore if we require any packages that require external libraries (such as GDAL)
it is left to the user to install and maintain their installation.

Give that most of our deployments are expected to happen via Docker images/containers,
we have full control over the system environment so this should not be an issue, but
it is something to be aware of depending on our future user base and deployments.
