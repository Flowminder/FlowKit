# Pytest for testing Flowkit components

Date: 20 July 2018

# Status

Pending

## Context

Testing is an important component of a fast changing software. Without testing
it is not possible to guarantee that new components or changes to the current
codebase do not break pre-existing functionality. The purpose of unit testing in
particular is to ensure that isolated parts of the codebase work as expected by
the developer. Tests should provide enough evidence to the developer that his
contributions are working. It also aids other developers to understand the
purpose of each and every component.

In order to reduce the burden of writing tests, we need good test management
tools that play well with `python` which is the main scripting language for this 
project.

There are many testing framework for Python, including:
[`nosetests`](https://nose.readthedocs.io/),
[`unittest`](https://docs.python.org/3/library/unittest.html) and
[`pytest`](https://docs.pytest.org/).

We were previously using `nosetests`, which makes use of classic xunit-style
setup whose main strength is easily declaring setup and teardown methods for
each testing function. While these methods are simple and familiar to those
coming from a unittest or nose background,  pytest’s more powerful fixture
mechanism leverages the concept of dependency injection, allowing for a more
modular and more scalable approach for managing test state, especially for
larger projects and for functional testing. 

In our particular case, we can leverage the power of fixtures to manage docker
images and to define connection procedures. Ideally, all of the testing
requirements are defined within the testing framework. Fixtures are really
powerful in providing powerful abstractions for resource allocation.

## Decision

We will use pytest for testing Flowkit components.

## Consequences

All of the tests should be written in compliance with `pytest`. We should
leverage fixtures to define resource allocation and key abstractions reducing
the burden of writting tests as much as possible.

Given that most of our development are expected to be in `python`, we can take 
full advantage of `pytest`.
