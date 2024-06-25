#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -euo pipefail


for VAR in "$FLOWDB_VERSION" "$FLOWDB_RELEASE_DATE"; do
    if ! [[ $VAR ]]; then
        echo "---------------------------------------------------"
        echo "                                                   "
        echo "  ERROR: One or more of the special environment    "
        echo "  is not defined. This will prevent Flowdb from     "
        echo "  building its image. Make sure to pass the        "
        echo "  following environment variables to your          "
        echo "  docker-compose.yml file or while building        "
        echo "  flowdb with docker build                          "
        echo "                                                   "
        echo "      FLOWDB_VERSION: Flowdb version number          "
        echo "      FLOWDB_RELEASE_DATE: Flowdb release date       "
        echo "                                                   "
        echo "  Pass those variables to the build process and    "
        echo "  try again.                                       "
        echo "                                                   "
        echo "---------------------------------------------------"
        exit 1
    fi
done

echo "----------------------------------------------"
echo " * All special environment variables are set."
echo "----------------------------------------------"
