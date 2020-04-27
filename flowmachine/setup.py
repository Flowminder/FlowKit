# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Setup configuration for `flowmachine`.

"""
import sys
import versioneer
from os import path as p

try:
    from setuptools import setup, find_packages

except ImportError:
    from distutils.core import setup

__status__ = "Development"
__author__ = "Flowminder Foundation"
__maintainer__ = "Flowminder Foundation"
__email__ = "flowkit@flowminder.org"


def read(filename, parent=None):
    """
    Reads a text file into memory.

    """
    parent = parent or __file__

    try:
        with open(p.join(p.dirname(parent), filename)) as f:
            return f.read()

    except IOError:
        return ""


#
#  Controls byte-compiling the shipped template.
#
sys.dont_write_bytecode = False

#
#  Parse all requirements.
#
readme = read("README.md")

# Test requirements

test_requirements = ["pytest", "pytest-cov", "pytest-asyncio", "asynctest"]

setup(
    name="flowmachine",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    entry_points={
        "console_scripts": ["flowmachine = flowmachine.core.server.server:main"]
    },
    description="Digestion program for Call Detail Record (CDR) data.",
    long_description=readme,
    py_module=["flowmachine"],
    author=__author__,
    author_email=__email__,
    url="https://github.com/Flowminder/FlowKit",
    keywords="mobile telecommunications analysis",
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=[
        "SQLAlchemy",
        "cachetools",
        "apispec-oneofschema",
        "marshmallow>=3.0.0",
        "marshmallow-oneofschema>=2.0.0",
        "numpy",
        "networkx",
        "pandas",
        "pglast",
        "python-dateutil",
        "pytz",
        "python-louvain",
        "psycopg2-binary",
        "finist",
        "redis",
        "pyzmq",
        "structlog",
        "shapely",
        "python-rapidjson",
        "get-secret-or-env-var",
    ],
    setup_requires=["pytest-runner"],
    tests_require=test_requirements,
    extras_require={"test": test_requirements, "uvloop": ["uvloop"]},
    python_require=">=3.7",
    include_package_data=True,
    zip_safe=False,
    platforms=["MacOS X", "Linux"],
    classifiers=[
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    ],
)
