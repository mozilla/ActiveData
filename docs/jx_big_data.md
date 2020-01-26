# Large Result Sets


## Introduction

ActiveData has a 50,000 record result set limit. The intent was to provide a humane amount of information; more information than a human can consume before the results of the next query are eady. It was expected the client would send more queries to drill down, or paginate, to give the illusion of large data.

This is not a good assumption for machine consumption: Machines can handle large result sets, and forcing the client to chunk requests is complicated, and less efficient.

## Requesting large responses

Add `destination: "url"` to the query object:

```json
{
    "from": "unittest",
    "limit": 200000,  
    "destination": "url",
    "format": "list"
}
```

Here is the response. It will contain a `url` which will have the eventual result.

```json
{
    "url": "https://active-data-query-results.s3-us-west-2.amazonaws.com/dih28UH.json",
    "status": "https://active-data-query-results.s3-us-west-2.amazonaws.com/dih28UH.status.json",
    "meta": {
        "es_query": {
            "_source": true,
            "from": 0,
            "query": {"match_all": {}},
            "size": 2000
        },
        "format": "list",
        "limit": 200000
    }
}
```

The response also has a `status` for monitoring progress, completion, or failure.

```json
{
    "status": "working",
    "row": 10000,
    "rows": 123000
}
```

The result is ready when `ok: true`:

```json
{
    "ok": true,
    "status": "done"
}
```

### Some important notes:

* `destination: "url"` - as mentioned before
* `format: "list"` - is required at this time (Jan 2020). More formatting options may be available in the future, but cubes and edge queries will never happen
* no `edges` - cubes of large data are too expensive at this time

## Large aggregations

Large aggregate results can also ba had, but they are limited to a single `groupby` column
 
```json
{
    "from": "unittest",
    "groupby": "result.test",
    "limit": 200000,  
    "destination": "url",
    "format": "list"
}
```

### Limit

* `destination: "url"` - as mentioned before
* `groupby` - is limited to one column, expressions are not allowed
* `format: "list"` - is required at this time (Jan 2020)
* no `edges` - cubes are not implemented


The response is almost identical; with `url` and `status`

```json
{
    "url": "https://active-data-query-results.s3-us-west-2.amazonaws.com/dih28UH.json",
    "status": "https://active-data-query-results.s3-us-west-2.amazonaws.com/dih28UH.status.json",
    "meta": {
        "es_query":{
            "aggs":{"_filter":{
                "aggs":{
                    "_match":{"aggs":{},"terms":{"field":"result.test.~s~","size":20}},
                    "_missing":{
                        "aggs":{},
                        "filter":{"bool":{"must_not":{"exists":{"field":"result.test.~s~"}}}}
                    }
                },
                "filter":{"match_all":{}}
            }},
            "size":0
        },
        "format": "list",
        "limit": 200000
    },
}
```

## Additional Features

* `chunk_size` - The number of records pulled from Elasticsearch at a time. This will make no difference to your result; as the chunk size has been set high enough to be efficient as possible. The default chunk size can be surmised by inspecting the `status`.  If your documents are very large (like in the case of `coverage`) setting the chunk size smaller (to `500`) will prevent download errors.
