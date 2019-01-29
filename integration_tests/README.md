# FlowKit Integration Tests

This folder contains tests which use a complete FlowKit system.

## Running the Tests

The integration test suite uses docker-compose, and pytest to respectively create the test environment, and run the tests. 

To run the tests, you should first run `docker-compose up`, in this directory. This will pull all necessary docker images, and start the containers. The API will start on port 9090 of localhost.

We recommend using [Pipenv](https://docs.pipenv.org) to run the tests, and a Pipfile and Pipfile.lock are included in this directory.
With Pipenv you can run the test suite as follows.

```bash
pipenv install
pipenv run pytest
```

If you are using an alternative environment manager, you should install the small number of packages listed in the Pipfile before running pytest.

The test suite makes use of a `.env` file, found in this directory. The values in this `.env` are used both by the test suite, and to create the docker containers. `docker-compose` will make use of the `.env` automatically, but you may need to explicitly supply the values if you are using a different method to spin up containers.

`pytest-dotenv` is used to load the `.env` for the test suite.