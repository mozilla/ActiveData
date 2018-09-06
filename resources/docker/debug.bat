#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/debug.sh


docker run --interactive --tty --env-file ./resources/docker/activedata.env -p 8001:8000/tcp --mount source=activedata_state,destination=/app/logs mozilla/activedata:v2.3rc20 bash


export PYTHONPATH=.:vendor
/usr/local/bin/gunicorn -b 0.0.0.0:$PORT --config=resources/docker/gunicorn.py active_data.app:flask_app


