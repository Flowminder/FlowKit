# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import io

from setuptools import find_packages, setup

import versioneer


with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="flowapi",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="http://github.com/Flowminder/FlowKit",
    license="MPLv2",
    maintainer="Flowminder",
    maintainer_email="flowkit@flowminder.org",
    description="FlowAPI provides a web API for communications with FlowMachine.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.7",
    install_requires=[
        "quart",
        "pyzmq",
        "hypercorn",
        "python-rapidjson",
        "structlog",
        "quart-jwt-extended[asymmetric_crypto]",
        "asyncpg",
        "pyyaml ==5.3.1",
        "jsonschema ==4.17.3",
        "apispec[yaml] < 6.7.1",  # Constrained to < 6.7.1 due to https://github.com/Flowminder/FlowKit/issues/6015#issuecomment-2627056217
        "get-secret-or-env-var",
        "prance[osv]",
        "openapi-schema-validator < 0.3.0",  # Pending https://github.com/Flowminder/FlowKit/issues/6015
        "werkzeug",
    ],
    extras_require={"test": ["pytest", "coverage"]},
)
