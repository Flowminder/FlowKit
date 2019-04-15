import json

import requests
from approvaltests import verify


def test_generated_openapi_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI.
    """
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi-redoc.json").json()
    spec_as_json_string = json.dumps(spec, indent=2)
    verify(spec_as_json_string, diff_reporter)
