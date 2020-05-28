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

test_requirements = ["pytest", "pytest-cov"]

setup(
    name="flowkit_jwt_generator",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Common test JWT generator for FlowKit.",
    entry_points={
        "console_scripts": ["generate-jwt = flowkit_jwt_generator.jwt:print_token"],
        "pytest11": ["flowkit_jwt_generator = flowkit_jwt_generator.fixtures"],
    },
    author=__author__,
    author_email=__email__,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Flowminder/FlowKit",
    keywords="mobile telecommunications analysis",
    packages=["flowkit_jwt_generator"],
    include_package_data=True,
    install_requires=[
        "pyjwt",
        "cryptography",
        "click",
        "requests",
        "pytest",
        "simplejson",
    ],
    extras_require={"test": test_requirements},
    tests_require=test_requirements,
    setup_requires=["pytest-runner"],
    python_requires=">=3.6",
    platforms=["MacOS X", "Linux"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.6",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    ],
)
