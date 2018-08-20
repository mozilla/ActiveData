# Docker

This directory is meant to build docker images for the `bugzilla-etl` branch.

## Instructions

All commands are meant to be run from the root directory for this repo; not this directory, rather its grandparent.

### Build

The `activedata.docker` file has default values for most `ARGS` to simplify the build, but you must provide a `REPO_CHECKOUT` to specify which branch, or tag you want to build.

    docker build \
        --file resources\docker\activedata.dockerfile 
        --build-arg REPO_CHECKOUT=bugzilla-etl
        --no-cache 
        --tag activedata .

*This command is also in the `build.sh` script*

**Notes**

* use `--build-arg REPO_CHECKOUT=tags/v2.0` to build a specific tag

### Run

Once the docker image is built, you may run it:

    docker run \
        --env-file ./resources/docker/activedata.env \ 
        --mount source=activedata_state,destination=/app/logs \ 
        -p 8000:8000/tcp 
        activedata

**Notes**

* The enviroment file (`activedata.env`) must reference an Elasticsearch cluster.
* The docker image requires inter-run state; both for logs and metadata storage; be sure to mount some (small) amount of storage to `/app/logs`. This has been hard coded.
* Notice the `-p 8000:8000/tcp` argument matches the `PORT` in `activedata.env`

### Configuration

The `activedata.json` file is the single source for all parameters required to run ActiveData. You probably do not need to change it. Notice it contains references to environment variables (eg. `{"$ref":"env://LOG_APPNAME"}`) and those variables are defined in the `activedata.env` file. 

    # CONTENTS OF activedata.env
    PORT=8000
    HOME=/app

    ES_HOST=http://host.docker.internal
    ES_PORT=9200

    ACTIVEDATA_CONFIG=resources/docker/activedata.json

    LOG_APPNAME=ActiveData

You must make your own version of this file for use by `docker run` 


### Troubleshoot

In the event that the container exits soon after starting, you can better see the reason by starting the container with `bash`

    docker run \
        --interactive \ 
        --tty \
        --env-file ./resources/docker/activedata.env \ 
        -p 8000:8000/tcp \
        --mount source=activedata_state,destination=/app/logs \ 
        activedata \
        bash

and running ActiveData directly

    export PYTHONPATH=.:vendor
    python active_data/app.py

gunicorn appears to be cutting off the stdout too early to capture the reason for shutdown
