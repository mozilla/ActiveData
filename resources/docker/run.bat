
REM RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run --interactive --tty --env-file ./resources/docker/activedata.env -p 8000:8000/tcp --mount source=activedata_state,destination=/app/logs activedata bash


docker run --env-file ./resources/docker/activedata.env -p 8000:8000/tcp --mount source=activedata_state,destination=/app/logs activedata
