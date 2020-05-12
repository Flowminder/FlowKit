# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import io

from setuptools import find_packages, setup

import versioneer

with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="flowauth",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="http://github.com/Flowminder/FlowKit",
    license="MPLv2",
    maintainer="Flowminder",
    maintainer_email="flowkit@flowminder.org",
    description="FlowAuth is a user and token management utility for use with FlowKit.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.7",
    install_requires=[
        "flask",
        "flask-sqlalchemy",
        "flask-login",
        "argon2_cffi",
        "passlib",
        "flask-principal",
        "pyjwt",
        "flask-wtf",
        "zxcvbn",
        "cryptography",
        "get-secret-or-env-var",
        "pyotp",
        "itsdangerous",
        "dogpile.cache",
        "simplejson",
    ],
    extras_require={
        "test": ["pytest", "coverage", "flowkit-jwt-generator"],
        "postgres": ["psycopg2-binary"],
        "mysql": ["mysqlclient"],
        "redis": ["redis"],
    },
)
