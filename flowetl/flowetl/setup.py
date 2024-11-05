# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import io
import os
import sys

from setuptools import find_packages, setup

sys.path.append(
    os.path.dirname(__file__)
)  # Workaround for https://github.com/warner/python-versioneer/issues/192
import versioneer

with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="flowetl",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="http://github.com/Flowminder/FlowKit",
    entry_points={
        "console_scripts": ["install-to-dag-folder=flowetl.cli:main"],
    },
    license="MPLv2",
    maintainer="Flowminder",
    maintainer_email="flowkit@flowminder.org",
    description="FlowETL is a collection of special purposes Airflow operators and sensors for use with FlowKit.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "apache-airflow[postgres] >= 2.0.0",
        "click",
    ],
    extras_require={
        "dev": [
            "structlog",
            "pytest",
            "psycopg2-binary",
            "pandas",
            "docker",
            "versioneer",
            "pytest-cov",
            "black",
        ]
    },
    python_requires=">=3.6",
)
