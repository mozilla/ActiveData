
REM RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run --interactive --tty --env-file ./resources/docker/activedata.env  -expose 8000:8000/tcp --mount source=activedata_state,destination=/app/logs activedata bash
