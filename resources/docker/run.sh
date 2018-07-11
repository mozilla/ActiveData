#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run \
       --interactive \
       --tty \
       --env-file ./resources/docker/activedata.env \
       --mount source=activedata_state,destination=/app/logs \
       -p 8000:8000/tcp \
       activedata \
       bash
