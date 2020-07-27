# ActiveData 

Provide high speed filtering and aggregation over data see [ActiveData Wiki Page](https://wiki.mozilla.org/Auto-tools/Projects/ActiveData) for project details


|Branch      |Status   | Coverage |
|------------|---------|----------|
|master      | [![Build Status](https://travis-ci.org/mozilla/ActiveData.svg?branch=master)](https://travis-ci.org/mozilla/ActiveData) | |
|dev         | [![Build Status](https://travis-ci.org/mozilla/ActiveData.svg?branch=dev)](https://travis-ci.org/mozilla/ActiveData)    | [![Coverage Status](https://coveralls.io/repos/github/mozilla/ActiveData/badge.svg)](https://coveralls.io/github/mozilla/ActiveData) |
|v1.7        | [![Build Status](https://travis-ci.org/mozilla/ActiveData.svg?branch=v1.7)](https://travis-ci.org/mozilla/ActiveData)    ||




## Use it now!

ActiveData is a service! You can certainly setup your own service, but it is easier to use Mozilla's!

    curl -XPOST -d "{\"from\":\"unittest\"}" http://activedata.allizom.org/query

## Requirements

* Python2.7 installed
* Elasticsearch **version 6.x**


### Elasticsearch Configuration

Elasticsearch has a configuration file at `config/elasticsearch.yml`. You must modify it to handle a high number of scripts 

    script.painless.regex.enabled: true
    script.max_compilations_rate: 10000/1m

We enable compression for faster transfer speeds

    http.compression: true

And it is a good idea to give your cluster a unique name so it does not join others on your local network

    cluster.name: lahnakoski_dev

then you can run Elasticsearch:
 
    c:\elasticsearch>bin\elasticsearch

Elasticsearch runs off port 9200. Test it is working 

    curl http://localhost:9200

you should expect something like 

    {
      "status" : 200,
      "name" : "dev",
      "cluster_name" : "lahnakoski_dev",
      "version" : {
        "number" : "1.7.5",
        "build_hash" : "00f95f4ffca6de89d68b7ccaf80d148f1f70e4d4",
        "build_timestamp" : "2016-02-02T09:55:30Z",
        "build_snapshot" : false,
        "lucene_version" : "4.10.4"
      },
      "tagline" : "You Know, for Search"
    }



## Installation

There is no PyPi install. Please clone `master` branch off of Github:

    git clone https://github.com/klahnakoski/ActiveData.git
    git checkout master

and install your requirements:

    pip install -r requirements.txt


## Configuration

The ActiveData service requires a configuration file that will point to the
default Elasticsearch index. You can find a few sample config files in
`resources/config`. `simple_settings.json` is simplest one:

```javascript
    {
        "flask":{
             "host":"0.0.0.0",
             "port":5000,
             "debug":false,
             "threaded":true,
             "processes":1
         },
        "constants":{
            "mo_http.http.default_headers":{"From":"https://wiki.mozilla.org/Auto-tools/Projects/ActiveData"}
        },
        "elasticsearch":{
            "host":"http://localhost",
            "port":9200,
            "index":"unittest",
            "type":"test_result",
            "debug":true
        }
        ...<snip>...
    }
```

The `elasticsearch` property must be updated to point to a specific cluster,
index and type. It is used as a default, and to find other indexes by name.

## Run

Jump to your git project directory, set your `PYTHONPATH` and run `app.py`:

```bash
    cd ~/ActiveData
    export PYTHONPATH=.:vendor
    python active_data/app.py --settings=resources/config/simple_settings.json
```

## Verify

If you have no records in your Elasticsearch cluster, then you must add some before you can query them.

Make a table in Elasticsearch, with one record: 

    curl -XPUT "http://localhost:9200/movies/movie/1" -d "{\"name\":\"The Parent Trap\",\"released\":\"29 July` 1998\",\"imdb\":\"http://www.imdb.com/title/tt0120783/\",\"rating\":\"PG\",\"director\":{\"name\":\"Nancy Meyers\",\"dob\":\"December 8, 1949\"}}"

Assuming you used the defaults, you can verify the service is up if you can
access the Query Tool at [http://localhost:5000/tools/query.html](http://localhost:5000/tools/query.html).
You may use it to send queries to your instance of the service. For example:

```javascript
    {"from":"movies"}
```

## Tests

The Github repo also included the test suite, and you can run it against
your service if you wish. **The tests will create indexes on your
cluster which are filled, queried, and destroyed**


**Linux**

```bash
    cd ~/ActiveData
    export PYTHONPATH=.:vendor
    python -m unittest discover -v -s tests
```

**Windows**

```cmd
    cd ActiveData
    SET PYTHONPATH=.:vendor
    python -m unittest discover -v -s tests
```
