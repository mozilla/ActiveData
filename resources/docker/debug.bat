#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/debug.sh


docker run --interactive --tty --env-file ./resources/docker/activedata.env -p 8000:8000/tcp --mount source=activedata_state,destination=/app/logs activedata bash
