#!/usr/bin/env bash


curl -XPOST http://localhost:9200/unittest/_search -d "{\"fields\":[\"etl.id\"],\"query\": {\"match_all\": {}},\"from\": 0,\"size\": 1}"
