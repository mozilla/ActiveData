# ActiveData
Provide high speed filtering and aggregation over data see [ActiveData Wiki Page](https://wiki.mozilla.org/Auto-tools/Projects/ActiveData) for project details

## Requirements

It is assumed you have Python2.7 installed.


## Installation

It is still too early for PyPi install, so please clone *master* off of github:

    git clone https://github.com/klahnakoski/ActiveData.git

and install your requirements:

    pip install -r requirements.txt


## Configuration

The ActiveData service requires a configuration file that will point to the
default ElasticSearch index.  You can find a few sample config files in
`resources/config`.  Here is simplest one:

    {
        "flask":{
             "host":"0.0.0.0",
             "port":5000,
             "debug":false,
             "threaded":true,
             "processes":1
         },
        "constants":{
            "pyLibrary.env.http.default_headers":{"From":"https://wiki.mozilla.org/Auto-tools/Projects/ActiveData"}
        },
        "elasticsearch":{
            "host":"http://localhost",
            "port":"9200",
            "index":"testdata",
            "type":"test_results",
            "debug":true
        }
    }

The `elasticsearch` property must be updated to point to a specific cluster,
index and type.  It is used as a default, and to find other indexes by name.

## Run

Jump to your git project directory, set your `PYTHONPATH` and run:

    cd ~/ActiveData
    export PYTHONPATH=.
    python active_data/app.py --settings=resources/config/simple_settings.json

## Verify

Assuming you used the defaults, you can verify the service is up if you can
access the Query Tool at [http://localhost:5000/tools/query.html](http://localhost:5000/tools/query.html).
You may use it to send queries to your instance of the service.  For example:

    {"from":"unittest"}

This query can be used on [AutoTool's public ActiveData instance](http://activedata.allizom.org/tools/query.html),
and you can use a similar query to get a few sample lines from your cluster.

## Tests

The Github repo also included the test suite, and you can run it against
your service if you wish.  **The tests will create indexes on your
cluster which are filled, queried, and destroyed**

```bash
    cd ~/ActiveData
    export PYTHONPATH=.
    # OPTIONAL, TEST_SETTINGS already defaults to this file
    set TEST_SETTINGS=tests/config/test_simple_settings.json
    python -m unittest discover -v -s tests
```
