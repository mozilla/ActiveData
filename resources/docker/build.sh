#!/usr/bin/env bash


# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker build \
       --file resources\docker\activedata.dockerfile \
       --no-cache \
       --tag activedata \
       .

