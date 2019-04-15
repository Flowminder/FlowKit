import json
import flowapi
import yaml
from flowmachine.utils import sort_recursively
import requests
from approvaltests import verify


def test_generated_openapi_redoc_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI (as formatted for redoc).
    """
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi-redoc.json").json()
    spec_version = spec["info"].pop("version")
    assert spec_version == flowapi.__version__
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    verify(spec_as_json_string, diff_reporter)


def test_generated_openapi_json_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI.
    """
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()
    spec_version = spec["info"].pop("version")
    assert spec_version == flowapi.__version__
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    verify(spec_as_json_string, diff_reporter)


def test_generated_openapi_yaml_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI in yaml form.
    """
    spec = yaml.load(requests.get(f"{flowapi_url}/api/0/spec/openapi.yaml").content)
    spec_version = spec["info"].pop("version")
    assert spec_version == flowapi.__version__
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    verify(spec_as_json_string, diff_reporter)
