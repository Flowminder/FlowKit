# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import io

from setuptools import find_packages, setup

import versioneer


with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="autoflow",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="http://github.com/Flowminder/FlowKit",
    license="MPLv2",
    maintainer="Flowminder",
    maintainer_email="flowkit@flowminder.org",
    description="AutoFlow automates event-driven Jupyter-notebook-based workflows that interact with FlowAPI.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "flowclient",
        "get-secret-or-env-var",
        "ipykernel",
        "marshmallow >= 3.0.0",
        "networkx",
        "nteract-scrapbook",
        "nbconvert>6.0.0",
        "nbformat",
        "papermill >= 1.2.1",
        "pendulum",
        "prefect ==0.10.7",  # Pinned pending updating for removal of storage.Memory
        "distributed ==2.19.0",  # Pinned because prefect is pinned"
        "pyyaml",
        "sh",
        "sqlalchemy<=1.3.23",
    ],
    extras_require={
        "test": ["pytest", "pytest-cov", "sqlalchemy-utils", "testing-postgresql"],
        "postgres": ["psycopg2-binary"],
        "examples": ["descartes", "geopandas", "matplotlib", "pandas"],
    },
)
