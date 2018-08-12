#!/usr/bin/env bash


# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker build \
       --file resources\docker\activedata.dockerfile \
       --build-arg REPO_CHECKOUT=bugzilla-etl \
       --no-cache \
       --tag activedata \
       .

