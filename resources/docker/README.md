# Docker

## Instructions

All commands are meant to be run from the root directory for this repo; not this directory, rather its grandparent.

### Build

The `activedata.docker` file has default values for most `ARGS` to simplify the build:

    docker build --file resources\docker\activedata.dockerfile --no-cache --tag activedata .

*This command is also in the `build.sh` script*

**Notes**

* use `--build-arg REPO_TAG=v2.0` to build a specific version

### Configuration

The `activedata.json` file is the single source for all parameters required to run ActiveData. You probably do not ned to change it. Notice it contains references to environment variables (eg. `{"$ref":"env://LOG_APPNAME"}`) and those variables are defined in the `activedata.env` files.

### Run

Once the docker image is built, you may run it:

    docker run 
        --env-file ./resources/docker/activedata.env 
        --mount source=activedata_state,destination=/app/logs 
        -p 8000:8000/tcp 
        activedata

This will not work unless the enviroment file (`activedata.env`) references an Elasticsearch cluster.

**Notes**

* Notice `PORT` in `activedata.env` matches the port of the `-p 8000:8000/tcp` option
* The docker image requires inter-run state; both for logs and metadata storage; be sure to mount some (small) amount of storage to `/app/logs`.  This has been hard coded.
