# FlowKit Integration Tests

This folder contains tests which use a complete FlowKit system.

## Running the Tests

The integration test suite uses docker-compose, and pytest to respectively create the test environment, and run the tests. 

We recommend using [Pipenv](https://docs.pipenv.org) to run the tests, and a Pipfile and Pipfile.lock are included in this directory.
With Pipenv you can run the test suite as follows.

```bash
pipenv install
pipenv run run-tests
```

This will pull all necessary docker images, start the containers and bring up instances of FlowMachine and FlowAPI. After the test suite has been run, the containers will be shut down agan.


If you are using an alternative environment manager, you should install the small number of packages listed in the Pipfile before running pytest.

The test suite makes use of the environment variables defined in `development_environment` in the project root. You will need to source these variables before running the tests.

### Running in PyCharm

To run the tests from within PyCharm, you will need to run `FLOWDB_SERVICES="flowdb_testdata" DOCKER_SERVICES="flowdb_testdata query_locker"` in the project root, and ensure you have provided the environment variables in the top level `development_environment` file to PyCharm (for example, by using the EnvFile plugin).