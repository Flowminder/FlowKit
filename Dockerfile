# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Worked examples docker container
# Provides all worked examples from the FlowKit docs in a ready-to-go JupyterLab install
#

FROM jupyter/scipy-notebook

RUN rm -rf /home/$NB_USER/work
ARG SOURCE_VERSION=0+uknown
ENV SOURCE_VERSION=${SOURCE_VERSION}
ENV SOURCE_TREE=FlowKit-${SOURCE_VERSION}
USER root
COPY --chown=$NB_UID:$NB_GID docs/source/worked_examples/*.ipynb /home/$NB_USER/
COPY --chown=$NB_UID:$NB_GID flowmachine /${SOURCE_TREE}/flowmachine
COPY --chown=$NB_UID:$NB_GID flowclient /${SOURCE_TREE}/flowclient
USER $NB_UID
RUN cd /${SOURCE_TREE}/flowclient && python setup.py bdist_wheel && \
    cd /${SOURCE_TREE}/flowmachine && python setup.py bdist_wheel
RUN pip install geopandas mapboxgl /${SOURCE_TREE}/flowclient/dist/*.whl \
     /${SOURCE_TREE}/flowmachine/dist/*.whl && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER && \
    cd /home/$NB_USER/ && jupyter trust -y *
