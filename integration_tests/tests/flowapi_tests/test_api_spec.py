# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import flowapi
import yaml
from flowmachine.utils import sort_recursively
import requests


def test_generated_openapi_json_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI.
    """
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()
    spec_version = spec["info"].pop("version")
    assert spec_version == flowapi.__version__
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    diff_reporter(spec_as_json_string)


def test_generated_openapi_yaml_spec(flowapi_url, diff_reporter):
    """
    Verify the OpenAPI spec for FlowAPI in yaml form.
    """
    spec = yaml.load(requests.get(f"{flowapi_url}/api/0/spec/openapi.yaml").content)
    spec_version = spec["info"].pop("version")
    assert spec_version == flowapi.__version__
    spec_as_json_string = json.dumps(sort_recursively(spec), indent=2, sort_keys=True)
    diff_reporter(spec_as_json_string)
