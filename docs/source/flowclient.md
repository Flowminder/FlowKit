# FlowClient

FlowClient is a Python client to FlowAPI. It can be installed using pip:

```bash
pip install flowclient
```

## Connecting to FlowAPI

To connect to a FlowKit server, you will need to generate a [JSON Web Token (JWT)](http://jwt.io) using [FlowAuth](../flowauth). You can produce one for test purposes using `token_generator.py` in the `integration_tests/tests` folder of the [GitHub repository](https://github.com/Flowminder/flowkit):

```bash
cd integration_tests
pipenv install
pipenv shell
python tests/token_generator.py -p daily_location run -p daily_location poll -p daily_location get_result -a daily_location admin3 -a daily_location admin2
eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NDA5MjMwOTksIm5iZiI6MTU0MDkyMzA5OSwianRpIjoiMjQ0NzZlMDgtYmQ4OS00M2Y2LWI0NmMtZWFlZWJiYTI3ZTA5IiwiZXhwIjoxNTQxMDA5NDk5LCJpZGVudGl0eSI6InRlc3QiLCJmcmVzaCI6dHJ1ZSwidHlwZSI6ImFjY2VzcyIsInVzZXJfY2xhaW1zIjp7ImRhaWx5X2xvY2F0aW9uIjp7InBlcm1pc3Npb25zIjpbInJ1biIsInBvbGwiLCJnZXRfcmVzdWx0Il0sInNwYXRpYWxfYWdncmVnYXRpb24iOlsiYWRtaW4zIiwiYWRtaW4yIl19fX0.SEu7xQxCYA05TlcC9meoWedhXaPQ5Raqn4tZ3UXifTE
```

The tool will look for a `JWT_SECRET_KEY` environment variable, or prompt you to enter one if that is unavailable. This should be the same key that is set for the flowapi docker container.

You can then create an API connection:

```python
import flowclient

conn = flowclient.Connection("http://localhost:9090", <JWT_STRING>)
```

## Documentation

FlowClient documentation can be found [here](../analyst/client/flowclient). Examples of FlowClient usage are provided [here](../analyst/example_usage).
