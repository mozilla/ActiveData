#!/usr/bin/env bash



curl -XDELETE 'http://localhost:9200/unittest20160516_141717/test_result/_query' -d '{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
