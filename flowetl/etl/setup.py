# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name="etl",
    url="https://github.com/Flowminder/FlowKit",
    maintainer="Flowminder",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    # pinning airflow version so that it is the same as in the Dockerfile
    install_requires=["apache-airflow[postgres]==1.10.3"],
)
