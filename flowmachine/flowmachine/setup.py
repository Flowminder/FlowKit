# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Setup configuration for `flowmachine_core`.

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


#
#  Controls byte-compiling the shipped template.
#
sys.dont_write_bytecode = False

#
#  Parse all requirements.
#
readme = """
Metapackage for flowmachine_core, which includes flowmachine-core, flowmachine-queries, and flowmachine-server.
"""


setup(
    name="flowmachine",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Digestion program for Call Detail Record (CDR) data.",
    long_description=readme,
    py_module=["flowmachine"],
    author=__author__,
    author_email=__email__,
    url="https://github.com/Flowminder/FlowKit",
    keywords="mobile telecommunications analysis",
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=["flowmachine-core", "flowmachine-queries", "flowmachine-server",],
    python_require=">=3.7",
    include_package_data=True,
    zip_safe=False,
    platforms=["MacOS X", "Linux"],
    classifiers=[
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    ],
)
