# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Setup configuration for `flowclient`.

"""
import versioneer

try:
    from setuptools import setup, find_packages

except ImportError:
    from distutils.core import setup

__status__ = "Development"
__author__ = "Flowminder Foundation"
__maintainer__ = "Flowminder Foundation"
__email__ = "flowkit@flowminder.org"
with open("README.md", "r") as fh:
    long_description = fh.read()

test_requirements = ["pytest", "pytest-cov", "asynctest", "pytest-asyncio"]

setup(
    name="flowclient",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Python client library for the FlowMachine API.",
    author=__author__,
    author_email=__email__,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Flowminder/FlowKit",
    keywords="mobile telecommunications analysis",
    packages=["flowclient"],
    include_package_data=True,
    install_requires=[
        "pandas",
        "requests",
        "pyjwt",
        "ujson",
        "merge-args",
        "tqdm",
        "ipywidgets",
    ],
    extras_require={"test": test_requirements},
    tests_require=test_requirements,
    setup_requires=["pytest-runner"],
    platforms=["MacOS X", "Linux"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    ],
)
