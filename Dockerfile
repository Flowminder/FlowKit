# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Worked examples docker container
# Provides all worked examples from the FlowKit docs in a ready-to-go JupyterLab install
#

FROM jupyter/scipy-notebook

RUN rm -rf /home/$NB_USER/work
ARG SOURCE_VERSION
ENV SOURCE_VERSION=${SOURCE_VERSION}
COPY docs/source/worked_examples/*.ipynb /home/$NB_USER/
COPY flowmachine /${SOURCE_TREE}/flowmachine
COPY flowclient /${SOURCE_VERSION}/flowclient
RUN cd /${SOURCE_VERSION}/flowclient && python setup.py bdist_wheel && \
    cd /${SOURCE_VERSION}/flowmachine && python setup.py bdist_wheel
RUN pip install geopandas mapboxgl /${SOURCE_VERSION}/flowclient/dist/*.whl \
     /${SOURCE_VERSION}/flowmachine/dist/*.whl && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER && \
    cd /home/$NB_USER/ && jupyter trust -y *
