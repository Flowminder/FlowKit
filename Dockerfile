FROM jupyter/datascience-notebook
RUN rm -rf /home/$NB_USER/work
COPY docs/source/worked_examples/*.ipynb /home/$NB_USER/
COPY flowmachine /flowmachine
COPY flowclient /flowclient
RUN pip install folium /flowclient /flowmachine && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER && \
    cd /home/$NB_USER/ && jupyter trust -y *