import json
from flowmachine.utils import sort_recursively
import requests
from approvaltests import verify


def test_generated_openapi_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI.
    """
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi-redoc.json").json()
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    verify(spec_as_json_string, diff_reporter)
