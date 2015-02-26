# ActiveData
Provide high speed filtering and aggregation over data

see [https://wiki.mozilla.org/Auto-tools/Projects/ActiveData](ActiveData Wiki Page) for project details

## Requirements

It is assumed you have cPython installed.


## Installation

It is still too early for PyPi install, so please clone master off of github:

    git clone https://github.com/klahnakoski/ActiveData.git

and install your requirements:

    pip install -r requirements.txt


## Configuration

The ActiveData service requires a configuration file that will point to the
default ElasticSearch index.  You can find a few sample config files in
`resources/config`.  Here is an excerpt of the one I use in development, and
that points to a local ES instance.

There are two references to ES, the first is for storing the request logs, and
the second is the default index

    ...
	"request_logs":{
		"host":"http://localhost",
		"port":"9200",
		"index":"active_data_requests",
		"type":"request_log",
		"schema":{"$ref":"//../schema/request_log.schema.json"}
	},
	"elasticsearch":{
		"host":"http://localhost",
		"port":"9200",
		"index":"testdata",
		"type":"test_results",
		"debug":true
	},
    ...

## Run

Jump to your git project directory, set your `PYTHONPATH` and run:

    cd ~/ActiveData
    export PYTHONPATH=.
    python active_data/app.py --settings=resources/config/simple_settings.json

## Verify

The [http://people.mozilla.org/~klahnakoski/qb/query.html](Qb Query Tool) can
be used to access your ActiveData instance.  Here is a sample query, but be
sure to change it to reflect your index name.

    <WORK IN PROGRESS>
