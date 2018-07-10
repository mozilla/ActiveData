# Docker

## Instructions

All commands are meant to be run from the root directory for this repo; not this directory, rather its grandparent.

### Build

```bash
docker build \
       --file resources\docker\activedata.dockerfile \
       --no-cache \
       --tag activedata \
       .
```

*This command is also in the `build.sh` script*


### Configuration

The `activedata.json` file is the single source for all parameters required to run ActiveData. It is properly configured for running both public and private ETL. Notice it contains references to environment variables (eg `{"$ref":"env://ETL_APPNAME"}`) and those variables are defined in the `*.env` files as examples. It is expected you will `docker run` with `-e` for each of those variables you want to override, or provide your own environment file with the secrets set.

### Run

Once the docker image is built, you may run it:

```bash
docker run \
       --interactive \
       --tty \
       --user app \
       --env-file ./resources/docker/activedata.env \
       --mount source=activedata_state,destination=/app/logs \
       activedata \
       bash
```

This will not work until you update the enviroment file (`activedata.env`) with suitable values.

**Notes**

* The environment variables file (`activedata.env`) lists all parameters you must set: Pointers to servers it touches, and values for the secrets.
* The docker image requires inter-run state; both for logs and the current ETL status; be sure to mount some (small) amount of storage to `/app/logs`.  You can change this in the `activedata.json` file
