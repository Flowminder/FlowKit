# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import io

from setuptools import find_packages, setup

import versioneer


with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="flowtrigger",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="http://github.com/Flowminder/FlowKit",
    license="MPLv2",
    maintainer="Flowminder",
    maintainer_email="flowkit@flowminder.org",
    description="FlowTrigger automates event-driven Jupyter-notebook-based workflows that interact with FlowAPI.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "flowclient",
        "pyyaml",
        "ipykernel",
        "papermill >= 1.2.1",
        "prefect",
        "nteract-scrapbook",
        "sh",
        "pendulum",
        "get-secret-or-env-var",
        "sqlalchemy",
        "networkx",
    ],
    extras_require={
        "test": [
            "pytest",
            "pytest-cov",
            "flowkit-jwt-generator",
            "testing-postgresql",
            "sqlalchemy-utils",
        ],
        "postgres": ["psycopg2-binary"],
        "examples": ["pandas", "geopandas", "descartes", "matplotlib"],
    },
)
