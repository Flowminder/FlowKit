FROM timescale/timescaledb:1.2.2-pg11

#
#  CONFIGURATION
#  -------------
#
#  In this section packages installed in previous
#  steps are properly configured. That happens by
#  either modifying configuration files (*.config)
#  or by loading *.sh scripts that will gradually
#  do that.
#
RUN mkdir -p /docker-entrypoint-initdb.d

#
#  Copy file spinup build scripts to be execed.
#
COPY --chown=postgres ./bin/build/* /docker-entrypoint-initdb.d/

#
#  Add local data to PostgreSQL data ingestion
#  directory. Files in that directory will be
#  ingested by PostgreSQL on build-time.
#
ADD --chown=postgres ./sql/* /docker-entrypoint-initdb.d/
# Need to make postgres owner
RUN chown -R postgres /docker-entrypoint-initdb.d

EXPOSE 5432
